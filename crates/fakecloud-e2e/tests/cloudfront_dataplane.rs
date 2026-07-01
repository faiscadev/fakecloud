// crates/fakecloud-e2e/tests/cloudfront_dataplane.rs
//! CloudFront data-plane scaffolding E2E tests.
//!
//! Task 1 only covers `StoredDistribution.bound_port` state plus the
//! `/_fakecloud/cloudfront/distributions` discovery route. No supervisor
//! exists yet, so `boundPort` is expected to be `null` here — it's wired
//! up in a later task.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    CacheBehavior, CacheBehaviors, CookiePreference, CustomErrorResponse, CustomErrorResponses,
    CustomOriginConfig, DefaultCacheBehavior, DistributionConfig, ForwardedValues, Headers,
    ItemSelection, Origin, OriginProtocolPolicy, Origins, ViewerProtocolPolicy,
};
use helpers::TestServer;
use std::time::Duration;

/// Creates a minimal SPA-style distribution: an S3-website default origin,
/// an optional `/api/*` custom origin cache behavior, and a 404 -> /index.html
/// (200) custom error response rule (the classic SPA fallback). Mirrors the
/// shape later data-plane tests reuse.
pub async fn make_spa_distribution(
    cf: &aws_sdk_cloudfront::Client,
    default_origin_domain: &str,
    api_origin_domain: Option<&str>,
) -> aws_sdk_cloudfront::types::Distribution {
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
        .caller_reference(format!("spa-{}", uuid_like()))
        .comment("spa e2e")
        .enabled(true)
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

    let config = config_builder.build().unwrap();

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
#[allow(dead_code)]
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
