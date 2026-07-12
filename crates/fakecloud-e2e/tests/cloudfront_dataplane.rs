// crates/fakecloud-e2e/tests/cloudfront_dataplane.rs
//! CloudFront data-plane E2E tests. Distributions are served on the server's
//! MAIN listener, routed by the `Host` header (their `<id>.cloudfront.net`
//! domain or an alias CNAME) -- there is no per-distribution ephemeral port.
//! These tests reach a distribution by sending a request to the main endpoint
//! with the distribution's domain as `Host`, and cover: introspection discovery
//! (`/_fakecloud/cloudfront/distributions`, the `served` flag), enable/disable
//! serving lifecycle, S3-website + custom-origin routing, alias/CNAME routing,
//! CustomErrorResponses (SPA fallback), startup rebind in persistent mode, and
//! that non-matching (AWS API) traffic is NOT intercepted.

// The SPA distribution config uses `ForwardedValues` (legacy, pre-cache-policy),
// which the AWS SDK marks deprecated in favor of CachePolicyId. It is the
// minimal valid shape for these tests, so allow the deprecation here.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    Aliases, CacheBehavior, CacheBehaviors, CookiePreference, CustomErrorResponse,
    CustomErrorResponses, CustomOriginConfig, DefaultCacheBehavior, DistributionConfig,
    ForwardedValues, Headers, ItemSelection, Origin, OriginProtocolPolicy, Origins,
    ViewerProtocolPolicy,
};
use helpers::TestServer;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// GET the main server endpoint with an explicit `Host` header -- how a viewer
/// reaches a distribution now that they are served on the main listener routed
/// by `Host`. `host` is the distribution's `<id>.cloudfront.net` domain (or an
/// alias CNAME).
async fn viewer_get(server: &TestServer, host: &str, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}{}", server.endpoint(), path))
        .header(reqwest::header::HOST, host)
        .send()
        .await
        .expect("viewer request sends")
}

/// Creates a minimal SPA-style distribution: an S3-website default origin,
/// an optional `/api/*` custom origin cache behavior, and a 404 -> /index.html
/// (200) custom error response rule (the classic SPA fallback). Mirrors the
/// shape later data-plane tests reuse.
pub async fn make_spa_distribution(
    cf: &aws_sdk_cloudfront::Client,
    default_origin_domain: &str,
    api_origin_domain: Option<&str>,
) -> aws_sdk_cloudfront::types::Distribution {
    let config = spa_config(
        default_origin_domain,
        api_origin_domain,
        &format!("spa-{}", uuid_like()),
        true,
        true,
        &[],
    );
    let create = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution");
    create
        .distribution()
        .expect("distribution returned")
        .clone()
}

/// Build the SPA distribution config. Shared by create (enabled=true) and the
/// disable-via-update path (enabled=false) so both use an identical shape; the
/// `caller_reference` must be preserved across an UpdateDistribution.
fn spa_config(
    default_origin_domain: &str,
    api_origin_domain: Option<&str>,
    caller_reference: &str,
    enabled: bool,
    with_error_rule: bool,
    aliases: &[&str],
) -> DistributionConfig {
    let mut origins_items = vec![Origin::builder()
        .id("o1")
        .domain_name(default_origin_domain)
        .build()
        .unwrap()];

    let mut cache_behaviors_items = Vec::new();
    if let Some(api_domain) = api_origin_domain {
        origins_items.push(
            Origin::builder()
                .id("o2")
                .domain_name(api_domain)
                .custom_origin_config(
                    CustomOriginConfig::builder()
                        .http_port(80)
                        .https_port(443)
                        .origin_protocol_policy(OriginProtocolPolicy::HttpOnly)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        );
        cache_behaviors_items.push(
            CacheBehavior::builder()
                // AWS path patterns are relative (no leading slash); the data
                // plane normalizes, so this must still match `/api/...`.
                .path_pattern("api/*")
                .target_origin_id("o2")
                .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                .forwarded_values(
                    ForwardedValues::builder()
                        .query_string(true)
                        .cookies(
                            CookiePreference::builder()
                                .forward(ItemSelection::All)
                                .build()
                                .unwrap(),
                        )
                        .headers(Headers::builder().quantity(0).build().unwrap())
                        .build()
                        .unwrap(),
                )
                .min_ttl(0)
                .build()
                .unwrap(),
        );
    }

    let origins_len = origins_items.len() as i32;
    let cache_behaviors_len = cache_behaviors_items.len() as i32;

    let mut config_builder = DistributionConfig::builder()
        .caller_reference(caller_reference)
        .comment("spa e2e")
        .enabled(enabled);

    if !aliases.is_empty() {
        let mut alias_builder = Aliases::builder().quantity(aliases.len() as i32);
        for a in aliases {
            alias_builder = alias_builder.items(*a);
        }
        config_builder = config_builder.aliases(alias_builder.build().unwrap());
    }

    config_builder = config_builder
        .origins(
            Origins::builder()
                .quantity(origins_len)
                .set_items(Some(origins_items))
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("o1")
                .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                .forwarded_values(
                    ForwardedValues::builder()
                        .query_string(false)
                        .cookies(
                            CookiePreference::builder()
                                .forward(ItemSelection::None)
                                .build()
                                .unwrap(),
                        )
                        .headers(Headers::builder().quantity(0).build().unwrap())
                        .build()
                        .unwrap(),
                )
                .min_ttl(0)
                .build()
                .unwrap(),
        );

    if with_error_rule {
        config_builder = config_builder.custom_error_responses(
            CustomErrorResponses::builder()
                .quantity(1)
                .set_items(Some(vec![CustomErrorResponse::builder()
                    .error_code(404)
                    .response_page_path("/index.html")
                    .response_code("200")
                    .error_caching_min_ttl(0)
                    .build()
                    .unwrap()]))
                .build()
                .unwrap(),
        );
    }

    if cache_behaviors_len > 0 {
        config_builder = config_builder.cache_behaviors(
            CacheBehaviors::builder()
                .quantity(cache_behaviors_len)
                .set_items(Some(cache_behaviors_items))
                .build()
                .unwrap(),
        );
    }

    config_builder.build().unwrap()
}

/// Cheap unique-enough suffix so repeated calls within a test don't collide
/// on CallerReference.
fn uuid_like() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Poll the introspection route until the distribution reports `served: true`.
/// Returns false if it never becomes served within the deadline.
async fn wait_for_served(server: &TestServer, dist_id: &str, deadline: Duration) -> bool {
    wait_for_served_state(server, dist_id, true, deadline).await
}

/// Poll until the distribution reports `served: false` (stopped serving).
async fn wait_for_unserved(server: &TestServer, dist_id: &str, deadline: Duration) -> bool {
    wait_for_served_state(server, dist_id, false, deadline).await
}

async fn wait_for_served_state(
    server: &TestServer,
    dist_id: &str,
    want: bool,
    deadline: Duration,
) -> bool {
    let url = format!("{}/_fakecloud/cloudfront/distributions", server.endpoint());
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(r) = client.get(&url).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if let Some(arr) = v.get("distributions").and_then(|x| x.as_array()) {
                    for d in arr {
                        if d.get("id").and_then(|x| x.as_str()) == Some(dist_id)
                            && d.get("served").and_then(|x| x.as_bool()) == Some(want)
                        {
                            return true;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn introspection_route_lists_distributions() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    // minimal S3-website origin distribution
    let dist = make_spa_distribution(
        &cf,
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;
    let url = format!("{}/_fakecloud/cloudfront/distributions", server.endpoint());
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = v["distributions"].as_array().unwrap();
    let entry = arr
        .iter()
        .find(|d| d["id"].as_str() == Some(dist.id()))
        .expect("distribution listed");
    // Enabled distribution is served on the main listener; the domain to reach
    // it is surfaced as `domainName` (the Host to send).
    assert_eq!(entry["served"].as_bool(), Some(true));
    assert_eq!(entry["domainName"].as_str(), Some(dist.domain_name()));
}

/// Disable a distribution: fetch its config + ETag, flip `enabled` to false,
/// and update. (Real CloudFront requires a distribution be disabled before it
/// can be deleted; the data plane must stop serving it either way.)
async fn disable_distribution(
    cf: &aws_sdk_cloudfront::Client,
    id: &str,
    default_origin_domain: &str,
    api_origin_domain: Option<&str>,
) {
    let got = cf
        .get_distribution_config()
        .id(id)
        .send()
        .await
        .expect("get_distribution_config");
    let etag = got.e_tag().expect("etag").to_string();
    let caller_reference = got
        .distribution_config()
        .expect("config")
        .caller_reference()
        .to_string();
    // Rebuild the same config shape with enabled=false (UpdateDistribution
    // requires the CallerReference be preserved).
    let cfg = spa_config(
        default_origin_domain,
        api_origin_domain,
        &caller_reference,
        false,
        true,
        &[],
    );
    cf.update_distribution()
        .id(id)
        .if_match(etag)
        .distribution_config(cfg)
        .send()
        .await
        .expect("update_distribution");
}

#[tokio::test]
async fn serves_on_enable_and_stops_on_disable() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let dist = make_spa_distribution(
        &cf,
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;

    // Enabled -> served on the main listener.
    assert!(
        wait_for_served(&server, dist.id(), Duration::from_secs(10)).await,
        "enabled distribution should be served"
    );

    // A viewer request routed by Host is answered (this asserts serving, not
    // origin content — the default origin here has no bucket, so the proxied
    // response is a 4xx/5xx; a successful send still proves the data plane
    // handled it). Origin content is covered by the S3-website test.
    viewer_get(&server, dist.domain_name(), "/").await;

    // Disable -> the data plane stops serving it (served flips to false) and its
    // Host no longer routes to a distribution.
    disable_distribution(
        &cf,
        dist.id(),
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;
    assert!(
        wait_for_unserved(&server, dist.id(), Duration::from_secs(10)).await,
        "disabled distribution should stop being served"
    );
}

/// Minimal HTTP origin that echoes the request path back, so a test can prove
/// the data plane routed to THIS origin (vs the S3 default). Mirrors the pattern
/// in `elbv2_dataplane.rs`.
struct EchoTarget {
    addr: std::net::SocketAddr,
}

impl EchoTarget {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|l| l.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_string();
                    let body = format!("ECHO {path}");
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        Self { addr }
    }
}

async fn put_object(s3: &aws_sdk_s3::Client, bucket: &str, key: &str, ctype: &str, body: &[u8]) {
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .content_type(ctype)
        .body(aws_sdk_s3::primitives::ByteStream::from(body.to_vec()))
        .send()
        .await
        .expect("put_object");
}

/// Create an S3 bucket configured as a website with `index.html` as both index
/// and error document (SPA setup), seeded with an index and a hashed asset.
async fn make_website_bucket(s3: &aws_sdk_s3::Client, bucket: &str) {
    s3.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create_bucket");
    s3.put_bucket_website()
        .bucket(bucket)
        .website_configuration(
            aws_sdk_s3::types::WebsiteConfiguration::builder()
                .index_document(
                    aws_sdk_s3::types::IndexDocument::builder()
                        .suffix("index.html")
                        .build()
                        .unwrap(),
                )
                .error_document(
                    aws_sdk_s3::types::ErrorDocument::builder()
                        .key("index.html")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("put_bucket_website");
    put_object(s3, bucket, "index.html", "text/html", b"<html>HOME</html>").await;
    put_object(
        s3,
        bucket,
        "assets/app.js",
        "application/javascript",
        b"APPJS",
    )
    .await;
}

#[tokio::test]
async fn serves_static_from_s3_website_origin_and_routes_api() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    make_website_bucket(&s3, "site").await;
    let api = EchoTarget::start().await;
    let api_domain = format!("127.0.0.1:{}", api.addr.port());

    let cf = server.cloudfront_client().await;
    let dist = make_spa_distribution(
        &cf,
        "site.s3-website-us-east-1.amazonaws.com",
        Some(&api_domain),
    )
    .await;
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);
    let host = dist.domain_name();

    // Default (S3-website) origin serves the hashed asset.
    let r = viewer_get(&server, host, "/assets/app.js").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "APPJS");

    // /api/* routes to the custom origin (echo proves it hit the API origin).
    let r = viewer_get(&server, host, "/api/orders").await;
    assert_eq!(r.text().await.unwrap(), "ECHO /api/orders");
}

#[tokio::test]
async fn stays_served_after_restart_persistent() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cf = server.cloudfront_client().await;
    let dist = make_spa_distribution(
        &cf,
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;
    let id = dist.id().to_string();

    assert!(
        wait_for_served(&server, &id, Duration::from_secs(10)).await,
        "distribution should be served before restart"
    );

    // Restart from the same data dir: the persisted enabled distribution is
    // served again immediately on the main listener (no per-distribution
    // listener to rebind, routing is derived from persisted state).
    server.restart().await;

    assert!(
        wait_for_served(&server, &id, Duration::from_secs(10)).await,
        "enabled distribution should stay served after restart in persistent mode"
    );
}

/// A distribution with an S3-website default origin and NO CustomErrorResponses,
/// so origin errors pass through unrewritten.
async fn make_static_distribution(
    cf: &aws_sdk_cloudfront::Client,
    default_origin_domain: &str,
) -> aws_sdk_cloudfront::types::Distribution {
    let config = spa_config(
        default_origin_domain,
        None,
        &format!("static-{}", uuid_like()),
        true,
        false,
        &[],
    );
    let create = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution");
    create.distribution().expect("distribution").clone()
}

#[tokio::test]
async fn spa_deep_route_falls_back_to_index_200() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    make_website_bucket(&s3, "spa").await;
    let cf = server.cloudfront_client().await;
    let dist = make_spa_distribution(&cf, "spa.s3-website-us-east-1.amazonaws.com", None).await;
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);

    // Deep client-side route: no such S3 key -> origin 404 -> CustomErrorResponse
    // (404 -> /index.html) served as 200.
    let r = viewer_get(&server, dist.domain_name(), "/orders/123").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "<html>HOME</html>");
}

#[tokio::test]
async fn missing_path_stays_404_without_error_rule() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    make_website_bucket(&s3, "static").await;
    let cf = server.cloudfront_client().await;
    let dist = make_static_distribution(&cf, "static.s3-website-us-east-1.amazonaws.com").await;
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);

    // With no CustomErrorResponses, a genuinely missing path keeps the origin's
    // 404 (the data plane does not rewrite it to 200).
    let r = viewer_get(&server, dist.domain_name(), "/assets/nope.js").await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn routes_by_alias_cname() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    make_website_bucket(&s3, "aliassite").await;
    let cf = server.cloudfront_client().await;

    // Distribution with an alternate domain name (CNAME). A viewer request whose
    // Host is the alias must route to this distribution -- a capability the old
    // per-distribution ephemeral-port design could not express.
    let alias = "cdn.example.test";
    let config = spa_config(
        "aliassite.s3-website-us-east-1.amazonaws.com",
        None,
        &format!("alias-{}", uuid_like()),
        true,
        true,
        &[alias],
    );
    let dist = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution")
        .distribution()
        .expect("distribution")
        .clone();
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);

    // Reachable both by the canonical <id>.cloudfront.net domain and by the alias.
    let r = viewer_get(&server, dist.domain_name(), "/").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "<html>HOME</html>");

    let r = viewer_get(&server, alias, "/").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "<html>HOME</html>");
}

#[tokio::test]
async fn api_traffic_is_not_intercepted() {
    // The viewer middleware must only intercept requests whose Host matches a
    // distribution. Normal AWS API calls (Host = the endpoint authority, not a
    // distribution domain) must pass straight through to dispatch even while a
    // distribution exists.
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let dist = make_spa_distribution(
        &cf,
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);

    // A CloudFront API call still works (proves the middleware passed it through).
    let listed = cf
        .list_distributions()
        .send()
        .await
        .expect("list_distributions must pass through the viewer middleware");
    let ids: Vec<_> = listed
        .distribution_list()
        .map(|l| l.items())
        .unwrap_or_default()
        .iter()
        .map(|d| d.id().to_string())
        .collect();
    assert!(ids.iter().any(|id| id == dist.id()));

    // An S3 call (different service, non-matching Host) also works.
    let s3 = server.s3_client().await;
    s3.list_buckets()
        .send()
        .await
        .expect("s3 list_buckets must pass through the viewer middleware");
}
