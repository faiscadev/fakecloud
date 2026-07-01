// crates/fakecloud-e2e/tests/cloudfront_dataplane.rs
//! CloudFront data-plane E2E tests: distribution discovery
//! (`/_fakecloud/cloudfront/distributions`) and the in-process data plane
//! (listener lifecycle: bind on enable, tear down on disable/delete, rebind on
//! startup in persistent mode). Origin routing + CustomErrorResponses are added
//! by later tasks.

// The SPA distribution config uses `ForwardedValues` (legacy, pre-cache-policy),
// which the AWS SDK marks deprecated in favor of CachePolicyId. It is the
// minimal valid shape for these tests, so allow the deprecation here.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    CacheBehavior, CacheBehaviors, CookiePreference, CustomErrorResponse, CustomErrorResponses,
    CustomOriginConfig, DefaultCacheBehavior, DistributionConfig, ForwardedValues, Headers,
    ItemSelection, Origin, OriginProtocolPolicy, Origins, ViewerProtocolPolicy,
};
use helpers::TestServer;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
                .path_pattern("/api/*")
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
        .enabled(enabled)
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
        )
        .custom_error_responses(
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

/// Poll the introspection route until the distribution reports a bound port.
async fn wait_for_bound_port(
    server: &TestServer,
    dist_id: &str,
    deadline: Duration,
) -> Option<u16> {
    let url = format!("{}/_fakecloud/cloudfront/distributions", server.endpoint());
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(r) = client.get(&url).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if let Some(arr) = v.get("distributions").and_then(|x| x.as_array()) {
                    for d in arr {
                        if d.get("id").and_then(|x| x.as_str()) == Some(dist_id) {
                            if let Some(p) = d.get("boundPort").and_then(|x| x.as_u64()) {
                                return Some(p as u16);
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
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
    assert!(arr.iter().any(|d| d["id"].as_str() == Some(dist.id())));
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
    );
    cf.update_distribution()
        .id(id)
        .if_match(etag)
        .distribution_config(cfg)
        .send()
        .await
        .expect("update_distribution");
}

/// Poll until the distribution is no longer reporting a bound port (listener
/// torn down). Returns false if it never unbinds within the deadline.
async fn wait_for_unbound(server: &TestServer, dist_id: &str, deadline: Duration) -> bool {
    let url = format!("{}/_fakecloud/cloudfront/distributions", server.endpoint());
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(r) = client.get(&url).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if let Some(arr) = v.get("distributions").and_then(|x| x.as_array()) {
                    let still_bound = arr.iter().any(|d| {
                        d.get("id").and_then(|x| x.as_str()) == Some(dist_id)
                            && d.get("boundPort").and_then(|x| x.as_u64()).is_some()
                    });
                    if !still_bound {
                        return true;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn binds_on_enable_and_stops_on_disable() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let dist = make_spa_distribution(
        &cf,
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;

    // Bind on enable: the supervisor allocates a listener and records the port.
    let port = wait_for_bound_port(&server, dist.id(), Duration::from_secs(10))
        .await
        .expect("data plane should bind a port for the enabled distribution");

    // The bound listener answers an HTTP request (this test asserts the
    // lifecycle, not origin content — the default origin here has no bucket, so
    // the proxied response is a 4xx/5xx; a successful send still proves the port
    // is serving). Origin content is covered by the S3-website test.
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/"))
        .send()
        .await
        .expect("bound listener answers");

    // Disable -> supervisor tears the listener down and clears bound_port.
    disable_distribution(
        &cf,
        dist.id(),
        "example-bucket.s3-website-us-east-1.amazonaws.com",
        None,
    )
    .await;
    assert!(
        wait_for_unbound(&server, dist.id(), Duration::from_secs(10)).await,
        "disabled distribution should stop serving / clear bound_port"
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
    let port = wait_for_bound_port(&server, dist.id(), Duration::from_secs(10))
        .await
        .expect("bind");
    let http = reqwest::Client::new();

    // Default (S3-website) origin serves the hashed asset.
    let r = http
        .get(format!("http://127.0.0.1:{port}/assets/app.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "APPJS");

    // /api/* routes to the custom origin (echo proves it hit the API origin).
    let r = http
        .get(format!("http://127.0.0.1:{port}/api/orders"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.text().await.unwrap(), "ECHO /api/orders");
}

#[tokio::test]
async fn rebinds_enabled_distribution_on_restart_persistent() {
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

    wait_for_bound_port(&server, &id, Duration::from_secs(10))
        .await
        .expect("distribution should bind before restart");

    // Restart from the same data dir: the persisted enabled distribution must
    // re-bind on the first supervisor tick (startup rebind).
    server.restart().await;

    wait_for_bound_port(&server, &id, Duration::from_secs(10))
        .await
        .expect("enabled distribution should rebind on startup in persistent mode");
}
