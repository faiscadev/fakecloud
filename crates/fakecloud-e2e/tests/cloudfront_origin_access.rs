//! CloudFront data plane fetching private S3 origins through an origin access
//! control (OAC) or a legacy origin access identity (OAI).
//!
//! Under `--iam strict` a bucket with no public access is readable through a
//! distribution only when its bucket policy grants the identity CloudFront
//! fetches as: the `cloudfront.amazonaws.com` service principal (scoped by
//! `aws:SourceArn` to the distribution) for an OAC, the OAI principal for an
//! OAI. An origin with neither is fetched anonymously and denied, as is an OAC
//! whose `SigningBehavior` is `never`. Default (no IAM) mode is unchanged.

// The distribution config uses `ForwardedValues` (legacy, pre-cache-policy),
// the minimal valid shape, which the AWS SDK marks deprecated.
#![allow(deprecated)]

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_cloudfront::types::{
    CacheBehavior, CacheBehaviors, CloudFrontOriginAccessIdentityConfig, CookiePreference,
    DefaultCacheBehavior, DistributionConfig, ForwardedValues, Headers, ItemSelection, Origin,
    OriginAccessControlConfig, OriginAccessControlOriginTypes, OriginAccessControlSigningBehaviors,
    OriginAccessControlSigningProtocols, Origins, S3OriginConfig, ViewerProtocolPolicy,
};
use helpers::TestServer;
use std::time::Duration;

const BUCKET: &str = "private-site";
const ORIGIN_DOMAIN: &str = "private-site.s3.us-east-1.amazonaws.com";

async fn strict_server() -> TestServer {
    TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await
}

/// A bucket with one object and no policy: private.
async fn private_bucket(s3: &aws_sdk_s3::Client) {
    s3.create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create_bucket");
    s3.put_object()
        .bucket(BUCKET)
        .key("index.html")
        .content_type("text/html")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"PRIVATE"))
        .send()
        .await
        .expect("put_object");
}

async fn create_oac(
    cf: &aws_sdk_cloudfront::Client,
    name: &str,
    behavior: OriginAccessControlSigningBehaviors,
) -> String {
    cf.create_origin_access_control()
        .origin_access_control_config(
            OriginAccessControlConfig::builder()
                .name(name)
                .origin_access_control_origin_type(OriginAccessControlOriginTypes::S3)
                .signing_behavior(behavior)
                .signing_protocol(OriginAccessControlSigningProtocols::Sigv4)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_origin_access_control")
        .origin_access_control()
        .expect("oac")
        .id()
        .to_string()
}

/// Returns `(id, S3CanonicalUserId)`.
async fn create_oai(cf: &aws_sdk_cloudfront::Client) -> (String, String) {
    let created = cf
        .create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(
            CloudFrontOriginAccessIdentityConfig::builder()
                .caller_reference(unique("oai"))
                .comment("e2e")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_cloud_front_origin_access_identity");
    let oai = created.cloud_front_origin_access_identity().expect("oai");
    (oai.id().to_string(), oai.s3_canonical_user_id().to_string())
}

/// A distribution whose single origin is the private bucket's REST endpoint,
/// with the given OAC id and/or OAI id.
async fn create_distribution(
    cf: &aws_sdk_cloudfront::Client,
    oac_id: Option<&str>,
    oai_id: Option<&str>,
) -> aws_sdk_cloudfront::types::Distribution {
    create_distribution_with_origin_path(cf, oac_id, oai_id, None).await
}

async fn create_distribution_with_origin_path(
    cf: &aws_sdk_cloudfront::Client,
    oac_id: Option<&str>,
    oai_id: Option<&str>,
    origin_path: Option<&str>,
) -> aws_sdk_cloudfront::types::Distribution {
    let origin = Origin::builder()
        .id("s3")
        .domain_name(ORIGIN_DOMAIN)
        .set_origin_path(origin_path.map(str::to_string))
        .set_origin_access_control_id(oac_id.map(str::to_string))
        .s3_origin_config(
            S3OriginConfig::builder()
                .origin_access_identity(
                    oai_id
                        .map(|id| format!("origin-access-identity/cloudfront/{id}"))
                        .unwrap_or_default(),
                )
                .build(),
        )
        .build()
        .unwrap();
    let config = DistributionConfig::builder()
        .caller_reference(unique("dist"))
        .comment("origin access e2e")
        .enabled(true)
        .origins(
            Origins::builder()
                .quantity(1)
                .items(origin)
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("s3")
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
        .build()
        .unwrap();
    cf.create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution")
        .distribution()
        .expect("distribution")
        .clone()
}

/// The bucket policy CDK's `S3BucketOrigin.withOriginAccessControl` writes.
fn oac_policy(distribution_arn: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "cloudfront.amazonaws.com"},
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{BUCKET}/*"),
            "Condition": {"StringEquals": {"AWS:SourceArn": distribution_arn}}
        }]
    })
    .to_string()
}

async fn put_policy(s3: &aws_sdk_s3::Client, policy: &str) {
    s3.put_bucket_policy()
        .bucket(BUCKET)
        .policy(policy)
        .send()
        .await
        .expect("put_bucket_policy");
}

async fn viewer_get(server: &TestServer, host: &str, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}{}", server.endpoint(), path))
        .header(reqwest::header::HOST, host)
        .send()
        .await
        .expect("viewer request sends")
}

/// Wait until the data plane serves the distribution, then GET `path` through it.
async fn get_through(
    server: &TestServer,
    dist: &aws_sdk_cloudfront::types::Distribution,
    path: &str,
) -> reqwest::Response {
    assert!(
        wait_for_served(server, dist.id(), Duration::from_secs(10)).await,
        "distribution {} never served",
        dist.id()
    );
    viewer_get(server, dist.domain_name(), path).await
}

async fn wait_for_served(server: &TestServer, dist_id: &str, deadline: Duration) -> bool {
    let url = format!("{}/_fakecloud/cloudfront/distributions", server.endpoint());
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(r) = client.get(&url).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                let served = v
                    .get("distributions")
                    .and_then(|x| x.as_array())
                    .is_some_and(|arr| {
                        arr.iter().any(|d| {
                            d.get("id").and_then(|x| x.as_str()) == Some(dist_id)
                                && d.get("served").and_then(|x| x.as_bool()) == Some(true)
                        })
                    });
                if served {
                    return true;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[tokio::test]
async fn oac_policy_scoped_to_the_distribution_allows_the_fetch() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let oac = create_oac(
        &cf,
        "oac-always",
        OriginAccessControlSigningBehaviors::Always,
    )
    .await;
    let dist = create_distribution(&cf, Some(&oac), None).await;

    // No bucket policy yet: the private bucket denies CloudFront.
    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 403, "private bucket without a grant");

    put_policy(&s3, &oac_policy(dist.arn())).await;
    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "PRIVATE");

    // The bucket itself stays private to anonymous callers.
    let direct = reqwest::get(format!("{}/{BUCKET}/index.html", server.endpoint()))
        .await
        .unwrap();
    assert_eq!(direct.status(), 403);
}

#[tokio::test]
async fn oac_policy_naming_another_distribution_denies_the_fetch() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let oac = create_oac(
        &cf,
        "oac-always",
        OriginAccessControlSigningBehaviors::Always,
    )
    .await;
    let granted = create_distribution(&cf, Some(&oac), None).await;
    let other = create_distribution(&cf, Some(&oac), None).await;
    put_policy(&s3, &oac_policy(granted.arn())).await;

    let r = get_through(&server, &other, "/index.html").await;
    assert_eq!(r.status(), 403, "SourceArn names a different distribution");
    let r = get_through(&server, &granted, "/index.html").await;
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn origin_without_oac_is_fetched_anonymously_and_denied() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let dist = create_distribution(&cf, None, None).await;
    // Even a grant naming this very distribution does not apply: without an
    // OAC CloudFront does not sign, so the request is anonymous.
    put_policy(&s3, &oac_policy(dist.arn())).await;

    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 403);
}

#[tokio::test]
async fn oac_signing_behavior_never_is_unsigned() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let oac = create_oac(&cf, "oac-never", OriginAccessControlSigningBehaviors::Never).await;
    let dist = create_distribution(&cf, Some(&oac), None).await;
    put_policy(&s3, &oac_policy(dist.arn())).await;

    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 403);
}

#[tokio::test]
async fn oac_no_override_signs_when_the_viewer_sends_no_authorization() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let oac = create_oac(
        &cf,
        "oac-no-override",
        OriginAccessControlSigningBehaviors::NoOverride,
    )
    .await;
    let dist = create_distribution(&cf, Some(&oac), None).await;
    put_policy(&s3, &oac_policy(dist.arn())).await;

    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "PRIVATE");
}

#[tokio::test]
async fn legacy_oai_principal_policy_allows_the_fetch() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let (oai_id, canonical) = create_oai(&cf).await;
    let dist = create_distribution(&cf, None, Some(&oai_id)).await;

    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 403, "no grant for the OAI yet");

    let by_arn = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": format!(
                "arn:aws:iam::cloudfront:user/CloudFront Origin Access Identity {oai_id}"
            )},
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{BUCKET}/*")
        }]
    });
    put_policy(&s3, &by_arn.to_string()).await;
    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "PRIVATE");

    let by_canonical = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"CanonicalUser": canonical},
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{BUCKET}/*")
        }]
    });
    put_policy(&s3, &by_canonical.to_string()).await;
    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 200);

    // A grant to the CloudFront service principal does not cover an OAI.
    put_policy(&s3, &oac_policy(dist.arn())).await;
    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 403);
}

#[tokio::test]
async fn default_mode_serves_private_origins_with_or_without_oac() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let oac = create_oac(
        &cf,
        "oac-always",
        OriginAccessControlSigningBehaviors::Always,
    )
    .await;
    let signed = create_distribution(&cf, Some(&oac), None).await;
    let unsigned = create_distribution(&cf, None, None).await;
    // A policy granting a different distribution changes nothing when IAM is
    // not enforced.
    put_policy(
        &s3,
        &oac_policy("arn:aws:cloudfront::000000000000:distribution/EOTHER"),
    )
    .await;

    for dist in [&signed, &unsigned] {
        let r = get_through(&server, dist, "/index.html").await;
        assert_eq!(r.status(), 200, "{}", dist.id());
        assert_eq!(r.text().await.unwrap(), "PRIVATE");
    }
}

/// A distribution in one account reading a private bucket another account owns
/// through an OAC: the bucket owner's policy grants the other account's
/// distribution, and the fetch is served from the bucket's account, not the
/// distribution's.
#[tokio::test]
async fn oac_reads_a_bucket_owned_by_another_account() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    private_bucket(&s3).await;

    let (akid, secret) = server.create_admin("222222222222", "cdn-admin").await;
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(akid, secret, None, None, "cdn-account"))
        .load()
        .await;
    let cf = aws_sdk_cloudfront::Client::new(&cfg);
    let oac = create_oac(
        &cf,
        "oac-cross",
        OriginAccessControlSigningBehaviors::Always,
    )
    .await;
    let dist = create_distribution(&cf, Some(&oac), None).await;
    assert!(dist.arn().contains(":222222222222:"), "{}", dist.arn());
    put_policy(&s3, &oac_policy(dist.arn())).await;

    let r = get_through(&server, &dist, "/index.html").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "PRIVATE");
}

/// A viewer path with dot segments stays under the origin's `OriginPath`: a
/// signed fetch must not reach objects outside it. Sent over a raw socket so
/// no client resolves the dot segments first.
#[tokio::test]
async fn viewer_dot_segments_stay_under_the_origin_path() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await; // `index.html` at the bucket root: outside /public
    s3.put_object()
        .bucket(BUCKET)
        .key("public/page.html")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"PUBLIC"))
        .send()
        .await
        .expect("put_object");
    let oac = create_oac(&cf, "oac-path", OriginAccessControlSigningBehaviors::Always).await;
    let dist = create_distribution_with_origin_path(&cf, Some(&oac), None, Some("/public")).await;
    put_policy(&s3, &oac_policy(dist.arn())).await;

    let r = get_through(&server, &dist, "/page.html").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "PUBLIC");

    for path in [
        "/%2e%2e/index.html",
        "/../index.html",
        "/..\\index.html",
        "/a\\..\\..\\index.html",
    ] {
        let resp = raw_viewer_get(&server, dist.domain_name(), path).await;
        assert!(
            !resp.contains("PRIVATE"),
            "{path} escaped the origin path: {resp}"
        );
        assert!(resp.starts_with("HTTP/1.1 404"), "{path}: {resp}");
    }
}

/// GET `path` through the distribution over a raw socket, so no client resolves
/// dot segments or rewrites backslashes first. Returns the raw response text.
async fn raw_viewer_get(server: &TestServer, host: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = server.endpoint().trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

/// The cache behavior is chosen on the dot-resolved viewer path, the one that
/// is fetched: `/assets/../index.html` is `/index.html`, which the default
/// behavior serves from an unsigned origin -- not the signed `/assets/*` one.
#[tokio::test]
async fn cache_behavior_is_matched_on_the_resolved_viewer_path() {
    let server = strict_server().await;
    let s3 = helpers::root_s3_client(&server).await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    s3.put_object()
        .bucket(BUCKET)
        .key("assets/app.js")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"APPJS"))
        .send()
        .await
        .expect("put_object");
    let oac = create_oac(
        &cf,
        "oac-assets",
        OriginAccessControlSigningBehaviors::Always,
    )
    .await;

    let s3_origin = |id: &str, oac: Option<&str>| {
        Origin::builder()
            .id(id)
            .domain_name(ORIGIN_DOMAIN)
            .set_origin_access_control_id(oac.map(str::to_string))
            .s3_origin_config(S3OriginConfig::builder().origin_access_identity("").build())
            .build()
            .unwrap()
    };
    let forwarded = || {
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
            .unwrap()
    };
    let config = DistributionConfig::builder()
        .caller_reference(unique("behaviors"))
        .comment("")
        .enabled(true)
        .origins(
            Origins::builder()
                .quantity(2)
                .items(s3_origin("signed", Some(&oac)))
                .items(s3_origin("anon", None))
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("anon")
                .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                .forwarded_values(forwarded())
                .min_ttl(0)
                .build()
                .unwrap(),
        )
        .cache_behaviors(
            CacheBehaviors::builder()
                .quantity(1)
                .items(
                    CacheBehavior::builder()
                        .path_pattern("assets/*")
                        .target_origin_id("signed")
                        .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                        .forwarded_values(forwarded())
                        .min_ttl(0)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let dist = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution")
        .distribution()
        .expect("distribution")
        .clone();
    put_policy(&s3, &oac_policy(dist.arn())).await;

    let r = get_through(&server, &dist, "/assets/app.js").await;
    assert_eq!(r.status(), 200, "the signed behavior serves its own path");
    for path in ["/assets/../index.html", "/assets/%2e%2e/index.html"] {
        let resp = raw_viewer_get(&server, dist.domain_name(), path).await;
        assert!(
            !resp.contains("PRIVATE"),
            "{path} read through the signed origin: {resp}"
        );
        assert!(resp.starts_with("HTTP/1.1 403"), "{path}: {resp}");
    }
}

/// A viewer request through an S3 origin only ever reaches the S3 service:
/// every path, however encoded, is an object key in the origin bucket, never
/// one of fakecloud's own routes (introspection, reset, IMDS, container
/// credentials, Cognito hosted endpoints).
#[tokio::test]
async fn s3_origin_paths_never_reach_fakecloud_internal_routes() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let dist = create_distribution(&cf, None, None).await;
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);
    let host = dist.domain_name();

    for path in [
        "/_fakecloud/cloudfront/distributions",
        "/%5ffakecloud/cloudfront/distributions",
        "/%5Ffakecloud/cloudfront/distributions",
        "//_fakecloud/cloudfront/distributions",
        "/./_fakecloud/cloudfront/distributions",
        "/x/../_fakecloud/cloudfront/distributions",
        "/_fakecloud/health",
        "/latest/meta-data/iam/security-credentials/",
        "/latest/meta-data/instance-id",
        "/latest/dynamic/instance-identity/document",
        "/v2/credentials/x",
        "/creds",
        "/us-east-1_abc/.well-known/jwks.json",
    ] {
        let resp = raw_viewer_get(&server, host, path).await;
        assert!(resp.starts_with("HTTP/1.1 404"), "{path}: {resp}");
        assert!(
            resp.contains("NoSuchKey"),
            "{path} was not served by S3: {resp}"
        );
        assert!(
            !resp.contains("distributions\""),
            "{path} reached introspection: {resp}"
        );
    }

    // POST /_reset through the distribution is an S3 request, not a reset.
    let resp = reqwest::Client::new()
        .post(format!("{}/_reset", server.endpoint()))
        .header(reqwest::header::HOST, host)
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 200, "POST /_reset through a distribution");
    let r = viewer_get(&server, host, "/index.html").await;
    assert_eq!(r.status(), 200, "state survived");
    assert_eq!(r.text().await.unwrap(), "PRIVATE");

    // An object that really lives under such a key is served normally.
    s3.put_object()
        .bucket(BUCKET)
        .key("_fakecloud/x")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"OBJECT"))
        .send()
        .await
        .expect("put_object");
    let r = viewer_get(&server, host, "/_fakecloud/x").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "OBJECT");
}

/// Viewer headers cannot steer an S3 origin fetch to another service: the
/// request is pinned to S3 whatever `X-Amz-Target` or `?Action=` say.
#[tokio::test]
async fn s3_origin_fetch_ignores_service_selecting_viewer_headers() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let cf = server.cloudfront_client().await;
    private_bucket(&s3).await;
    let dist = create_distribution(&cf, None, None).await;
    assert!(wait_for_served(&server, dist.id(), Duration::from_secs(10)).await);

    let r = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header(reqwest::header::HOST, dist.domain_name())
        .header("x-amz-target", "DynamoDB_20120810.ListTables")
        .header("content-type", "application/x-amz-json-1.0")
        .body("{}")
        .send()
        .await
        .unwrap();
    let body = r.text().await.unwrap();
    assert!(!body.contains("TableNames"), "reached DynamoDB: {body}");

    let r = viewer_get(&server, dist.domain_name(), "/index.html?Action=ListUsers").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "PRIVATE");
}
