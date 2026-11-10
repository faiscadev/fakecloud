//! Domain endpoints served on fakecloud's own listener, routed by `Host`.
//!
//! A domain reports its AWS-format endpoint
//! (`search-<name>-<id>.<region>.es.amazonaws.com`). Like a CloudFront
//! distribution's `<id>.cloudfront.net`, that name is served by fakecloud's
//! main port: a request whose `Host` is a live domain's endpoint (or its
//! `EndpointV2`) is proxied to the domain's backing cluster, so a client that
//! points the name at fakecloud (`/etc/hosts`, a compose network alias, or
//! just a `Host` header) talks to the domain the way it would on AWS, and it
//! works even when the cluster's own published port isn't reachable from the
//! client (fakecloud running in a container). Every other request falls
//! through untouched. A domain whose cluster isn't up answers 503.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};

use crate::state::SharedOpenSearchState;

/// What a request's `Host` resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// The `Host` names no domain: not ours.
    NotADomain,
    /// A domain whose cluster isn't running (yet): ours, but unavailable.
    Unavailable,
    /// Proxy to this cluster base URL.
    Cluster(String),
}

/// Hop-by-hop headers, plus the ones the proxy rewrites itself.
const SKIPPED_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

pub struct OpenSearchDataPlane {
    state: SharedOpenSearchState,
    client: reqwest::Client,
}

impl OpenSearchDataPlane {
    /// `None` if the HTTP client can't be built (the proxy then never
    /// intercepts).
    pub fn new(state: SharedOpenSearchState) -> Option<Arc<Self>> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(120))
            .build()
            .ok()?;
        Some(Arc::new(Self { state, client }))
    }

    /// Resolve a request's `Host` (with or without a port).
    pub fn route(&self, host: &str) -> Route {
        let host = normalize(host);
        // Every domain endpoint is `search-...`; anything else (all AWS API
        // traffic) is rejected without touching the state lock.
        if !host.starts_with("search-") {
            return Route::NotADomain;
        }
        let accounts = self.state.read();
        for (_, st) in accounts.iter() {
            for d in st.domains.values() {
                if d.deleted || d.endpoint.is_empty() {
                    continue;
                }
                let endpoint = d.endpoint.to_ascii_lowercase();
                if host == endpoint || host == format!("{endpoint}.v2") {
                    return match d.reachable_url() {
                        Some(url) => Route::Cluster(url),
                        None => Route::Unavailable,
                    };
                }
            }
        }
        Route::NotADomain
    }

    /// Forward one request to `base` (a [`Route::Cluster`] URL).
    pub async fn forward(
        &self,
        base: &str,
        method: Method,
        path_and_query: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response<Bytes> {
        let url = format!("{}{path_and_query}", base.trim_end_matches('/'));
        let mut req = self.client.request(method, &url);
        for (name, value) in headers {
            if !SKIPPED_HEADERS.contains(&name.as_str()) {
                req = req.header(name, value);
            }
        }
        let upstream = match req.body(body).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%url, error = %e, "search domain proxy: cluster unreachable");
                return unavailable("The domain's search cluster did not respond.");
            }
        };
        let status = upstream.status();
        let mut out_headers = HeaderMap::new();
        for (name, value) in upstream.headers() {
            if !SKIPPED_HEADERS.contains(&name.as_str()) {
                out_headers.append(name, value.clone());
            }
        }
        let bytes = match upstream.bytes().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%url, error = %e, "search domain proxy: response body failed");
                return unavailable("The domain's search cluster response was cut short.");
            }
        };
        let mut resp = Response::new(bytes);
        *resp.status_mut() = status;
        *resp.headers_mut() = out_headers;
        resp
    }
}

/// The 503 a domain answers while its cluster isn't running.
pub fn unavailable(message: &str) -> Response<Bytes> {
    let body = serde_json::json!({ "message": message }).to_string();
    let mut resp = Response::new(Bytes::from(body));
    *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    resp
}

fn normalize(host: &str) -> String {
    let host = host.trim();
    // Strip a port, leaving bracketed IPv6 literals alone (never a domain).
    let host = match host.rsplit_once(':') {
        Some((h, port)) if !h.contains(']') && port.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host,
    };
    host.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Domain, DomainDataPlane};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn domain(name: &str, endpoint: &str, up: bool) -> Domain {
        let mut d: Domain = serde_json::from_value(serde_json::json!({
            "name": name,
            "domain_id": format!("123456789012/{name}"),
            "arn": format!("arn:aws:es:us-east-1:123456789012:domain/{name}"),
            "engine_version": "OpenSearch_2.11",
            "created_via_es": false,
            "endpoint": endpoint,
            "created": true,
            "deleted": false,
            "config": {},
            "tags": {},
            "created_at": "2026-01-01T00:00:00Z",
        }))
        .unwrap();
        if up {
            d.data_plane = Some(DomainDataPlane {
                container_id: "c1".into(),
                host: "127.0.0.1".into(),
                port: 49200,
            });
        }
        d
    }

    fn plane(domains: Vec<Domain>) -> Arc<OpenSearchDataPlane> {
        let state: SharedOpenSearchState = Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )));
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create("123456789012");
            for d in domains {
                st.domains.insert(d.name.clone(), d);
            }
        }
        OpenSearchDataPlane::new(state).unwrap()
    }

    #[test]
    fn a_domain_endpoint_routes_to_its_cluster_and_other_hosts_fall_through() {
        let dp = plane(vec![
            domain("up", "search-up-abc.us-east-1.es.amazonaws.com", true),
            domain(
                "booting",
                "search-booting-def.us-east-1.es.amazonaws.com",
                false,
            ),
        ]);
        let cluster = Route::Cluster("http://127.0.0.1:49200".into());
        assert_eq!(
            dp.route("search-up-abc.us-east-1.es.amazonaws.com"),
            cluster
        );
        assert_eq!(
            dp.route("SEARCH-UP-ABC.us-east-1.es.amazonaws.com:4566"),
            cluster
        );
        assert_eq!(
            dp.route("search-up-abc.us-east-1.es.amazonaws.com.v2"),
            cluster
        );
        assert_eq!(
            dp.route("search-booting-def.us-east-1.es.amazonaws.com"),
            Route::Unavailable
        );
        for host in [
            "localhost:4566",
            "es.us-east-1.amazonaws.com",
            "search-gone-x.us-east-1.es.amazonaws.com",
            "[::1]:4566",
        ] {
            assert_eq!(dp.route(host), Route::NotADomain, "{host}");
        }
    }

    #[test]
    fn a_deleted_domain_no_longer_routes() {
        let mut d = domain("up", "search-up-abc.us-east-1.es.amazonaws.com", true);
        d.deleted = true;
        let dp = plane(vec![d]);
        assert_eq!(
            dp.route("search-up-abc.us-east-1.es.amazonaws.com"),
            Route::NotADomain
        );
    }

    #[tokio::test]
    async fn requests_are_forwarded_with_method_path_headers_and_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let mut got = Vec::new();
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got);
                if let Some(idx) = text.find("\r\n\r\n") {
                    if text[idx + 4..].len() >= 7 {
                        break;
                    }
                }
            }
            let body = b"{\"ok\":true}";
            sock.write_all(
                format!(
                    "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\nx-cluster: yes\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
            sock.write_all(body).await.unwrap();
            String::from_utf8(got).unwrap()
        });
        let dp = plane(vec![]);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert(
            "host",
            "search-x.us-east-1.es.amazonaws.com".parse().unwrap(),
        );
        let resp = dp
            .forward(
                &format!("http://{addr}/"),
                Method::PUT,
                "/idx/_doc/1?refresh=true",
                &headers,
                Bytes::from_static(b"{\"a\":1}"),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers()["x-cluster"], "yes");
        assert_eq!(resp.body().as_ref(), b"{\"ok\":true}");
        let request = server.await.unwrap();
        assert!(
            request.starts_with("PUT /idx/_doc/1?refresh=true HTTP/1.1\r\n"),
            "{request}"
        );
        assert!(
            request.contains("content-type: application/json"),
            "{request}"
        );
        assert!(!request.contains("search-x.us-east-1"), "{request}");
        assert!(request.ends_with("{\"a\":1}"), "{request}");
    }

    #[tokio::test]
    async fn an_unreachable_cluster_answers_503() {
        let dp = plane(vec![]);
        let resp = dp
            .forward(
                "http://127.0.0.1:1",
                Method::GET,
                "/",
                &HeaderMap::new(),
                Bytes::new(),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
