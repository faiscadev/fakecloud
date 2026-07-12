// crates/fakecloud-cloudfront/src/dataplane.rs
//! In-process CloudFront data plane.
//!
//! Distributions are served on fakecloud's **main `--addr` listener**, routed by
//! the request `Host` header, rather than on a per-distribution ephemeral port.
//! [`CloudFrontDataPlane::serve`] is installed as an outer middleware on the main
//! axum router: it matches the `Host` header against every enabled distribution's
//! `DomainName` (`<id>.cloudfront.net`) or one of its alternate domain names
//! (`Aliases`/CNAMEs). A match is served as viewer traffic; anything else (the AWS
//! API, `/_fakecloud/*`, health) is handed straight back for normal dispatch.
//!
//! This is how real CloudFront works -- a distribution is reached by its domain,
//! not a port -- and it means a distribution is reachable from outside a container
//! whenever the main port is published (`-p`), with no second listener to expose.
//! Clients discover which distributions are served, and the domain to send as
//! `Host`, via `/_fakecloud/cloudfront/distributions`.
//!
//! Once a request is matched to a distribution, [`serve`](CloudFrontDataPlane::serve)
//! selects a cache behavior by path pattern, resolves its origin, reverse-proxies
//! to it, and applies CustomErrorResponses (e.g. the SPA `404 -> /index.html`
//! served as `200`). There is no global edge network -- this is a single local
//! origin-serving node, matching the ALB/API Gateway precedent. Deferred (not
//! implemented): in-path CloudFront Functions / Lambda@Edge, TTL caching /
//! invalidation, and OAC/SigV4 to private S3.

use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use bytes::Bytes;
use http::{header, HeaderMap, Method, StatusCode};
use tracing::{trace, warn};

use crate::model::DistributionConfig;
use crate::state::{CloudFrontAccounts, SharedCloudFrontState, StoredDistribution};

const ENV_DISABLE: &str = "FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE";

/// Whether the data plane should serve viewer traffic. Disabled by setting
/// `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE` to a truthy value (mirrors the ELBv2
/// flag), for environments that only exercise the control plane. Also drives the
/// `served` flag surfaced via `/_fakecloud/cloudfront/distributions`.
pub fn dataplane_enabled() -> bool {
    !matches!(
        std::env::var(ENV_DISABLE).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// The CloudFront data plane: serves enabled distributions on the main listener,
/// routed by `Host`. Constructed once at server startup and installed as an outer
/// middleware; see [`CloudFrontDataPlane::serve`].
pub struct CloudFrontDataPlane {
    state: SharedCloudFrontState,
    /// HTTP client used to fetch from origins (reverse-proxy).
    upstream: reqwest::Client,
    /// `host:port` of fakecloud's own server. An S3-website origin is served by
    /// this same process on the main port, so those origins are reached here with
    /// the website domain preserved in the `Host` header (real CloudFront likewise
    /// treats an S3-website endpoint as an HTTP custom origin).
    s3_endpoint: String,
    /// Cached `dataplane_enabled()` at construction: when false, `serve` never
    /// intercepts and every request falls through to normal AWS dispatch.
    enabled: bool,
}

impl CloudFrontDataPlane {
    /// Build the data plane. `server_port` is fakecloud's own listen port, used to
    /// reach S3-website origins served by this same process. Returns an `Arc` for
    /// sharing into the axum middleware layer. Cheap and infallible; if the tuned
    /// reqwest client fails to build (should not happen), the plane declines to
    /// serve (`enabled = false`) so requests still dispatch normally rather than
    /// being proxied through a degraded client.
    pub fn new(state: SharedCloudFrontState, server_port: u16) -> std::sync::Arc<Self> {
        let mut enabled = dataplane_enabled();
        let upstream = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|e| {
                warn!(
                    "CloudFront data plane: failed to build reqwest client: {e}; serving disabled"
                );
                // Decline to serve: a default client lacks the invalid-cert /
                // no-redirect / timeout behavior the data plane relies on, so
                // proxying through it would silently misbehave.
                enabled = false;
                reqwest::Client::new()
            });
        std::sync::Arc::new(Self {
            state,
            upstream,
            s3_endpoint: format!("127.0.0.1:{server_port}"),
            enabled,
        })
    }

    /// Serve a request iff its `Host` matches an enabled distribution.
    ///
    /// - `Ok(resp)`  -- the request was viewer traffic for a distribution and was
    ///   proxied to the resolved origin (the body has been consumed).
    /// - `Err(req)`  -- the `Host` matches no distribution (or the plane is
    ///   disabled); the request is returned untouched for normal AWS dispatch.
    ///
    /// The `Host` check happens on the request headers before the body is touched,
    /// so pass-through traffic (all AWS API calls, `/_fakecloud/*`) is never
    /// buffered.
    pub async fn serve(&self, req: Request<Body>) -> Result<Response, Request<Body>> {
        if !self.enabled {
            return Err(req);
        }
        // Prefer the `Host` header (HTTP/1.1); fall back to the URI authority so
        // HTTP/2 viewer requests (which carry the domain in `:authority` and may
        // omit `Host`) still route to a distribution.
        let host = req
            .headers()
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| req.uri().host().map(|h| h.to_string()));
        let Some(host) = host else {
            return Err(req);
        };

        // Resolve the route under the read lock (owned snapshot so the guard drops
        // at the end of the block). The outer `Option` distinguishes "no
        // distribution serves this Host" (fall through) from "a distribution
        // matched but has no usable origin" (serve a 502 -- it IS our traffic).
        let matched: Option<Option<RouteResolution>> = {
            let accs = self.state.read();
            find_distribution_by_host(&accs, &host)
                .map(|d| resolve_route(&d.config, req.uri().path(), &self.s3_endpoint))
        };
        let Some(route_opt) = matched else {
            return Err(req);
        };

        // From here the request belongs to CloudFront: consume it and proxy.
        let (parts, body) = req.into_parts();
        // Apply the SAME buffered-body cap as direct (non-viewer) traffic
        // (`FAKECLOUD_MAX_REQUEST_BODY_BYTES`, default 1 GiB) so a request isn't
        // rejected merely because it went through a distribution.
        let max_body = fakecloud_core::dispatch::max_request_body_bytes();
        let body_bytes = match axum::body::to_bytes(body, max_body).await {
            Ok(b) => b,
            Err(_) => {
                return Ok(canned(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "viewer request body too large",
                ))
            }
        };
        let Some(route) = route_opt else {
            return Ok(canned(
                StatusCode::BAD_GATEWAY,
                "distribution has no matching origin",
            ));
        };

        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
            .to_string();
        let url = format!("{}{path_and_query}", route.upstream.url_base);
        trace!(%host, path = %parts.uri.path(), origin = %route.upstream.host_header, "CloudFront data plane: proxying");
        let resp = self
            .fetch_origin(
                &parts.method,
                &url,
                &route.upstream.host_header,
                &parts.headers,
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
            let err_resp = self
                .fetch_origin(
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
                    .and_then(|c| StatusCode::from_u16(c).ok())
                    .unwrap_or(origin_status);
                *err_resp.status_mut() = final_status;
                return Ok(err_resp);
            }
            return Ok(resp);
        }
        Ok(resp)
    }

    /// Reverse-proxy the request to the resolved origin and copy the response back.
    async fn fetch_origin(
        &self,
        method: &Method,
        url: &str,
        host_header: &str,
        req_headers: &HeaderMap,
        body: &Bytes,
    ) -> Response {
        let mut rb = self.upstream.request(reqwest_method(method), url);
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
                let mut builder = Response::builder().status(status);
                for (k, v) in headers.iter() {
                    if !is_hop_by_hop(k.as_str()) {
                        builder = builder.header(k, v);
                    }
                }
                builder
                    .body(Body::from(bytes))
                    .unwrap_or_else(|_| canned(StatusCode::BAD_GATEWAY, "invalid origin response"))
            }
            Err(e) => canned(StatusCode::BAD_GATEWAY, &format!("origin error: {e}")),
        }
    }
}

/// Find the enabled distribution whose `DomainName` (`<id>.cloudfront.net`) or one
/// of its alternate domain names (`Aliases`/CNAMEs) matches `host`. The port is
/// stripped and matching is case-insensitive. Alternate domain names are exact in
/// CloudFront (not wildcards), so this is an exact host compare, mirroring the
/// route53 CloudFront resolver.
pub(crate) fn find_distribution_by_host<'a>(
    accs: &'a CloudFrontAccounts,
    host: &str,
) -> Option<&'a StoredDistribution> {
    let host = host.split(':').next().unwrap_or(host).trim();
    if host.is_empty() {
        return None;
    }
    accs.all_distributions()
        .map(|(_, d)| d)
        .filter(|d| d.config.enabled)
        .find(|d| {
            d.domain_name.eq_ignore_ascii_case(host)
                || d.config
                    .aliases
                    .as_ref()
                    .and_then(|a| a.items.as_ref())
                    .is_some_and(|it| it.cname.iter().any(|c| c.eq_ignore_ascii_case(host)))
        })
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

fn canned(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(msg.to_string()))
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
    use crate::model::{AliasItems, Aliases, CustomOriginConfig, Origin};
    use crate::state::StoredDistribution;
    use chrono::Utc;

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

    fn dist(id: &str, enabled: bool, aliases: &[&str]) -> StoredDistribution {
        let mut config = DistributionConfig {
            enabled,
            ..Default::default()
        };
        if !aliases.is_empty() {
            config.aliases = Some(Aliases {
                quantity: aliases.len() as i32,
                items: Some(AliasItems {
                    cname: aliases.iter().map(|s| s.to_string()).collect(),
                }),
            });
        }
        StoredDistribution {
            id: id.to_string(),
            arn: format!("arn:aws:cloudfront::123456789012:distribution/{id}"),
            status: "Deployed".into(),
            last_modified_time: Utc::now(),
            domain_name: format!("{}.cloudfront.net", id.to_lowercase()),
            in_progress_invalidation_batches: 0,
            etag: "E1".into(),
            config,
        }
    }

    fn accounts_with(dists: Vec<StoredDistribution>) -> CloudFrontAccounts {
        let mut accs = CloudFrontAccounts::new();
        let acct = accs.entry("123456789012");
        for d in dists {
            acct.distributions.insert(d.id.clone(), d);
        }
        accs
    }

    #[test]
    fn find_by_domain_name() {
        let accs = accounts_with(vec![dist("E1ABC", true, &[])]);
        let found = find_distribution_by_host(&accs, "e1abc.cloudfront.net").unwrap();
        assert_eq!(found.id, "E1ABC");
    }

    #[test]
    fn find_strips_port_and_is_case_insensitive() {
        let accs = accounts_with(vec![dist("E1ABC", true, &[])]);
        assert!(find_distribution_by_host(&accs, "E1ABC.CloudFront.net:4566").is_some());
    }

    #[test]
    fn find_by_alias_cname() {
        let accs = accounts_with(vec![dist("E1ABC", true, &["cdn.example.com"])]);
        let found = find_distribution_by_host(&accs, "cdn.example.com").unwrap();
        assert_eq!(found.id, "E1ABC");
    }

    #[test]
    fn disabled_distribution_is_not_matched() {
        let accs = accounts_with(vec![dist("E1ABC", false, &["cdn.example.com"])]);
        assert!(find_distribution_by_host(&accs, "e1abc.cloudfront.net").is_none());
        assert!(find_distribution_by_host(&accs, "cdn.example.com").is_none());
    }

    #[test]
    fn unknown_host_and_empty_host_return_none() {
        let accs = accounts_with(vec![dist("E1ABC", true, &[])]);
        assert!(find_distribution_by_host(&accs, "s3.amazonaws.com").is_none());
        assert!(find_distribution_by_host(&accs, "").is_none());
        assert!(find_distribution_by_host(&accs, ":4566").is_none());
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
}
