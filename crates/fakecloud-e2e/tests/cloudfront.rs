//! CloudFront E2E tests against the AWS Rust SDK.
//!
//! `forwarded_values` / `min_ttl` are deprecated on the SDK builder in
//! favour of policy IDs, but keeping the legacy fields exercised in our
//! roundtrip tests makes sure CloudFront responses remain backwards
//! compatible for older SDK clients.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::builders::DistributionConfigBuilder;
use aws_sdk_cloudfront::types::{
    Aliases, CacheBehavior, CacheBehaviors, CachedMethods, ConnectionMode, CookiePreference,
    CustomErrorResponse, CustomErrorResponses, DefaultCacheBehavior, DistributionConfig,
    DistributionConfigWithTags, ForwardedValues, GeoRestriction, GeoRestrictionType, Headers,
    HttpVersion, ItemSelection, LoggingConfig, Method, MinimumProtocolVersion, Origin, Origins,
    Paths, PriceClass, Restrictions, SslSupportMethod, Tags, ViewerCertificate,
    ViewerProtocolPolicy,
};
use helpers::TestServer;

fn minimal_config(caller_ref: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("e2e")
        .enabled(true)
        .origins(
            Origins::builder()
                .quantity(1)
                .items(
                    Origin::builder()
                        .id("primary")
                        .domain_name("example.com")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("primary")
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
                .default_ttl(86_400)
                .max_ttl(31_536_000)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

#[tokio::test]
async fn cloudfront_create_then_get_distribution() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let config = minimal_config("e2e-create");
    let create = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution");
    let dist = create.distribution().expect("distribution returned");
    let id = dist.id().to_string();
    assert!(!id.is_empty());
    assert!(dist.arn().contains(&id));
    assert!(dist.domain_name().ends_with(".cloudfront.net"));
    let etag = create.e_tag().expect("etag header");
    assert!(!etag.is_empty());

    let got = cf
        .get_distribution()
        .id(&id)
        .send()
        .await
        .expect("get_distribution");
    assert_eq!(got.distribution().unwrap().id(), id);
}

#[tokio::test]
async fn cloudfront_list_distributions() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_distribution()
        .distribution_config(minimal_config("e2e-list"))
        .send()
        .await
        .expect("create_distribution");

    let list = cf
        .list_distributions()
        .send()
        .await
        .expect("list_distributions");
    let lst = list.distribution_list().expect("list shape");
    assert!(lst.quantity() >= 1);
    let items = lst.items();
    assert!(!items.is_empty());
}

#[tokio::test]
async fn cloudfront_update_distribution_requires_if_match() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-update"))
        .send()
        .await
        .expect("create_distribution");
    let dist = create.distribution().unwrap();
    let id = dist.id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    let mut new_config = minimal_config("e2e-update");
    new_config = DistributionConfig::builder()
        .caller_reference(new_config.caller_reference())
        .comment("e2e-updated")
        .enabled(true)
        .origins(new_config.origins().unwrap().clone())
        .default_cache_behavior(new_config.default_cache_behavior().unwrap().clone())
        .build()
        .unwrap();

    cf.update_distribution()
        .id(&id)
        .if_match(&etag)
        .distribution_config(new_config)
        .send()
        .await
        .expect("update_distribution");

    let got = cf.get_distribution().id(&id).send().await.unwrap();
    assert_eq!(
        got.distribution()
            .unwrap()
            .distribution_config()
            .unwrap()
            .comment(),
        "e2e-updated"
    );
}

/// CloudFront errors use the REST-XML `<ErrorResponse>` wrapper, so the SDK
/// parses them into the typed modeled error (#2632).
#[tokio::test]
async fn cloudfront_errors_are_typed_sdk_errors() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let err = cf
        .get_distribution()
        .id("EDOESNOTEXIST")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_no_such_distribution(), "{err:?}");
    assert_eq!(err.code(), Some("NoSuchDistribution"));
    assert!(
        err.message().is_some_and(|m| !m.is_empty()),
        "message must be parsed: {err:?}"
    );

    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-typed-err"))
        .send()
        .await
        .expect("create_distribution");
    let id = create.distribution().unwrap().id().to_string();
    let err = cf
        .update_distribution()
        .id(&id)
        .if_match("ESTALEETAG")
        .distribution_config(minimal_config("e2e-typed-err"))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    // Recorded from real AWS on 2026-11-09 for `UpdateDistribution`.
    assert!(err.is_invalid_if_match_version(), "{err:?}");
}

#[tokio::test]
async fn cloudfront_create_invalidation() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-inv"))
        .send()
        .await
        .expect("create_distribution");
    let id = create.distribution().unwrap().id().to_string();
    let inv = cf
        .create_invalidation()
        .distribution_id(&id)
        .invalidation_batch(
            aws_sdk_cloudfront::types::InvalidationBatch::builder()
                .caller_reference("inv-1")
                .paths(
                    Paths::builder()
                        .quantity(1)
                        .items("/static/*")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_invalidation");
    assert!(inv.invalidation().unwrap().id().starts_with('I'));

    let list = cf
        .list_invalidations()
        .distribution_id(&id)
        .send()
        .await
        .expect("list_invalidations");
    assert_eq!(list.invalidation_list().unwrap().quantity(), 1);
}

#[tokio::test]
async fn cloudfront_tags_roundtrip() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-tags"))
        .send()
        .await
        .expect("create_distribution");
    let arn = create.distribution().unwrap().arn().to_string();
    cf.tag_resource()
        .resource(&arn)
        .tags(
            aws_sdk_cloudfront::types::Tags::builder()
                .items(
                    aws_sdk_cloudfront::types::Tag::builder()
                        .key("env")
                        .value("prod")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("tag_resource");

    let list = cf
        .list_tags_for_resource()
        .resource(&arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    let tags = list.tags().unwrap().items();
    assert!(tags
        .iter()
        .any(|t| t.key() == "env" && t.value() == Some("prod")));

    cf.untag_resource()
        .resource(&arn)
        .tag_keys(
            aws_sdk_cloudfront::types::TagKeys::builder()
                .items("env")
                .build(),
        )
        .send()
        .await
        .expect("untag_resource");

    let after = cf
        .list_tags_for_resource()
        .resource(&arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    let tags = after.tags().unwrap().items();
    assert!(!tags.iter().any(|t| t.key() == "env"));
}

#[tokio::test]
async fn cloudfront_complex_config_roundtrips() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let config = DistributionConfig::builder()
        .caller_reference("complex")
        .comment("complex")
        .enabled(true)
        .price_class(PriceClass::PriceClass100)
        .http_version(HttpVersion::Http2)
        .is_ipv6_enabled(true)
        .origins(
            Origins::builder()
                .quantity(1)
                .items(
                    Origin::builder()
                        .id("origin-1")
                        .domain_name("origin.example.com")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("origin-1")
                .viewer_protocol_policy(ViewerProtocolPolicy::RedirectToHttps)
                .compress(true)
                .allowed_methods(
                    aws_sdk_cloudfront::types::AllowedMethods::builder()
                        .quantity(2)
                        .items(Method::Get)
                        .items(Method::Head)
                        .cached_methods(
                            CachedMethods::builder()
                                .quantity(2)
                                .items(Method::Get)
                                .items(Method::Head)
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                )
                .min_ttl(0)
                .default_ttl(86_400)
                .max_ttl(31_536_000)
                .default_ttl(86400)
                .max_ttl(31536000)
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
                .build()
                .unwrap(),
        )
        .cache_behaviors(
            CacheBehaviors::builder()
                .quantity(1)
                .items(
                    CacheBehavior::builder()
                        .path_pattern("/api/*")
                        .target_origin_id("origin-1")
                        .viewer_protocol_policy(ViewerProtocolPolicy::HttpsOnly)
                        .min_ttl(0)
                        .default_ttl(86_400)
                        .max_ttl(31_536_000)
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
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .custom_error_responses(
            CustomErrorResponses::builder()
                .quantity(1)
                .items(
                    CustomErrorResponse::builder()
                        .error_code(403)
                        .response_page_path("/403.html")
                        .response_code("403")
                        .error_caching_min_ttl(10)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .restrictions(
            Restrictions::builder()
                .geo_restriction(
                    GeoRestriction::builder()
                        .restriction_type(GeoRestrictionType::Whitelist)
                        .quantity(1)
                        .items("US")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .viewer_certificate(
            ViewerCertificate::builder()
                .cloud_front_default_certificate(true)
                .minimum_protocol_version(MinimumProtocolVersion::TlSv122021)
                .build(),
        )
        .build()
        .unwrap();

    let create = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .expect("create_distribution");
    let id = create.distribution().unwrap().id().to_string();

    let got = cf
        .get_distribution_config()
        .id(&id)
        .send()
        .await
        .expect("get_distribution_config");
    let cfg = got.distribution_config().unwrap();
    assert_eq!(cfg.cache_behaviors().unwrap().quantity(), 1);
    assert_eq!(cfg.custom_error_responses().unwrap().quantity(), 1);
    assert_eq!(
        cfg.restrictions()
            .unwrap()
            .geo_restriction()
            .unwrap()
            .restriction_type(),
        &GeoRestrictionType::Whitelist
    );
    assert_eq!(cfg.price_class(), Some(&PriceClass::PriceClass100));
    // Members AWS spells with an upper-case acronym (MinTTL, IsIPV6Enabled,
    // ErrorCachingMinTTL) must survive the parse and the render.
    assert_eq!(cfg.is_ipv6_enabled(), Some(true));
    let dcb = cfg.default_cache_behavior().unwrap();
    assert_eq!(dcb.min_ttl(), Some(0));
    assert_eq!(dcb.default_ttl(), Some(86400));
    assert_eq!(dcb.max_ttl(), Some(31536000));
    assert_eq!(
        cfg.custom_error_responses().unwrap().items()[0].error_caching_min_ttl(),
        Some(10)
    );
}

/// The fewest members `CreateDistribution` accepts: every optional envelope is
/// left for CloudFront to fill in.
fn bare_config(caller_ref: &str) -> DistributionConfig {
    bare_builder(caller_ref).build().unwrap()
}

fn bare_builder(caller_ref: &str) -> DistributionConfigBuilder {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("")
        .enabled(true)
        .origins(
            Origins::builder()
                .quantity(1)
                .items(
                    Origin::builder()
                        .id("primary")
                        .domain_name("example.com")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("primary")
                .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                .cache_policy_id(MANAGED_CACHE_POLICY)
                .build()
                .unwrap(),
        )
}

/// What the AWS provider for Terraform dereferences unguarded, and what the
/// AWS CLI's `create-distribution` example shows CloudFront returning.
fn assert_cloudfront_defaults(cfg: &DistributionConfig) {
    assert_eq!(cfg.aliases().unwrap().quantity(), 0);
    assert_eq!(cfg.origin_groups().unwrap().quantity(), 0);
    assert_eq!(cfg.cache_behaviors().unwrap().quantity(), 0);
    assert_eq!(cfg.custom_error_responses().unwrap().quantity(), 0);
    let geo = cfg.restrictions().unwrap().geo_restriction().unwrap();
    assert_eq!(geo.restriction_type(), &GeoRestrictionType::None);
    assert_eq!(geo.quantity(), 0);
    let vc = cfg.viewer_certificate().unwrap();
    assert_eq!(vc.cloud_front_default_certificate(), Some(true));
    assert_eq!(
        vc.minimum_protocol_version(),
        Some(&MinimumProtocolVersion::TlSv1)
    );
    assert_eq!(cfg.price_class(), Some(&PriceClass::PriceClassAll));
    assert_eq!(cfg.http_version(), Some(&HttpVersion::Http2));
    assert_eq!(cfg.is_ipv6_enabled(), Some(true));
    assert_eq!(cfg.web_acl_id(), Some(""));
    let logging = cfg.logging().unwrap();
    assert!(!logging.enabled());

    let dcb = cfg.default_cache_behavior().unwrap();
    assert_eq!(dcb.trusted_signers().unwrap().quantity(), 0);
    assert_eq!(dcb.trusted_key_groups().unwrap().quantity(), 0);
    assert_eq!(dcb.lambda_function_associations().unwrap().quantity(), 0);
    assert_eq!(dcb.function_associations().unwrap().quantity(), 0);
    let allowed = dcb.allowed_methods().unwrap();
    assert_eq!(allowed.items(), &[Method::Head, Method::Get]);
    assert_eq!(
        allowed.cached_methods().unwrap().items(),
        &[Method::Head, Method::Get]
    );

    let origin = &cfg.origins().unwrap().items()[0];
    assert_eq!(origin.custom_headers().unwrap().quantity(), 0);
    assert_eq!(origin.connection_attempts(), Some(3));
    assert_eq!(origin.connection_timeout(), Some(10));
}

#[tokio::test]
async fn cloudfront_bare_config_reads_back_with_cloudfront_defaults() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(bare_config("e2e-defaults"))
        .send()
        .await
        .expect("create_distribution");
    let created = create.distribution().unwrap();
    assert_cloudfront_defaults(created.distribution_config().unwrap());
    let id = created.id().to_string();

    let got = cf.get_distribution().id(&id).send().await.unwrap();
    assert_cloudfront_defaults(got.distribution().unwrap().distribution_config().unwrap());

    let got = cf.get_distribution_config().id(&id).send().await.unwrap();
    assert_cloudfront_defaults(got.distribution_config().unwrap());

    let list = cf.list_distributions().send().await.unwrap();
    let summary = &list.distribution_list().unwrap().items()[0];
    assert_eq!(summary.origin_groups().unwrap().quantity(), 0);
    assert_eq!(summary.aliases().unwrap().quantity(), 0);
    assert_eq!(
        summary
            .restrictions()
            .unwrap()
            .geo_restriction()
            .unwrap()
            .restriction_type(),
        &GeoRestrictionType::None
    );
    assert_eq!(
        summary
            .viewer_certificate()
            .unwrap()
            .cloud_front_default_certificate(),
        Some(true)
    );
    // Enum case, unlike the config's `http2` (recorded from real AWS 2026-11-09).
    assert_eq!(summary.http_version().as_str(), "HTTP2");
    assert!(summary.is_ipv6_enabled());
    assert_eq!(summary.connection_mode(), Some(&ConnectionMode::Direct));
}

#[tokio::test]
async fn cloudfront_config_read_back_and_updated_is_a_no_op() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(bare_config("e2e-noop"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();

    let got = cf.get_distribution_config().id(&id).send().await.unwrap();
    let etag = got.e_tag().unwrap().to_string();
    let updated = cf
        .update_distribution()
        .id(&id)
        .if_match(&etag)
        .distribution_config(got.distribution_config().unwrap().clone())
        .send()
        .await
        .expect("the config GetDistributionConfig returns is accepted");
    assert_eq!(updated.e_tag(), Some(etag.as_str()));
    assert_cloudfront_defaults(
        updated
            .distribution()
            .unwrap()
            .distribution_config()
            .unwrap(),
    );

    // So is the omitted form: it means the defaults, not a change.
    let updated = cf
        .update_distribution()
        .id(&id)
        .if_match(&etag)
        .distribution_config(bare_config("e2e-noop"))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.e_tag(), Some(etag.as_str()));
}

#[tokio::test]
async fn cloudfront_update_omitting_a_member_resets_it_to_the_default() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let config = bare_builder("e2e-reset")
        .price_class(PriceClass::PriceClass100)
        .http_version(HttpVersion::Http3)
        .is_ipv6_enabled(false)
        .build()
        .unwrap();
    let create = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .unwrap();
    let dist = create.distribution().unwrap();
    let kept = dist.distribution_config().unwrap();
    assert_eq!(kept.price_class(), Some(&PriceClass::PriceClass100));
    assert_eq!(kept.http_version(), Some(&HttpVersion::Http3));
    assert_eq!(kept.is_ipv6_enabled(), Some(false));

    let updated = cf
        .update_distribution()
        .id(dist.id())
        .if_match(create.e_tag().unwrap())
        .distribution_config(bare_config("e2e-reset"))
        .send()
        .await
        .unwrap();
    assert_ne!(updated.e_tag(), create.e_tag());
    assert_cloudfront_defaults(
        updated
            .distribution()
            .unwrap()
            .distribution_config()
            .unwrap(),
    );
}

#[tokio::test]
async fn cloudfront_partial_viewer_certificates_are_completed() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let default_only = bare_builder("e2e-cert-default")
        .viewer_certificate(
            ViewerCertificate::builder()
                .cloud_front_default_certificate(true)
                .build(),
        )
        .build()
        .unwrap();
    let create = cf
        .create_distribution()
        .distribution_config(default_only)
        .send()
        .await
        .unwrap();
    let vc = create
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap()
        .viewer_certificate()
        .unwrap();
    assert_eq!(
        vc.minimum_protocol_version(),
        Some(&MinimumProtocolVersion::TlSv1)
    );

    let arn = "arn:aws:acm:us-east-1:123456789012:certificate/abc";
    let acm = bare_builder("e2e-cert-acm")
        .viewer_certificate(
            ViewerCertificate::builder()
                .acm_certificate_arn(arn)
                .ssl_support_method(SslSupportMethod::SniOnly)
                .minimum_protocol_version(MinimumProtocolVersion::TlSv122021)
                .build(),
        )
        .build()
        .unwrap();
    let create = cf
        .create_distribution()
        .distribution_config(acm)
        .send()
        .await
        .unwrap();
    let vc = create
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap()
        .viewer_certificate()
        .unwrap();
    assert_eq!(vc.cloud_front_default_certificate(), Some(false));
    assert_eq!(vc.acm_certificate_arn(), Some(arn));
    assert_eq!(vc.ssl_support_method(), Some(&SslSupportMethod::SniOnly));
    assert_eq!(
        vc.minimum_protocol_version(),
        Some(&MinimumProtocolVersion::TlSv122021)
    );
}

#[tokio::test]
async fn cloudfront_certificate_mirrors_follow_a_read_modify_write() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(bare_config("e2e-cert-rmw"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();

    let got = cf.get_distribution_config().id(&id).send().await.unwrap();
    let read = got.distribution_config().unwrap();
    let arn = "arn:aws:acm:us-east-1:123456789012:certificate/abc";
    let edited = bare_builder("e2e-cert-rmw")
        .viewer_certificate(switched_to_acm(read.viewer_certificate().unwrap(), arn))
        .build()
        .unwrap();
    let updated = cf
        .update_distribution()
        .id(&id)
        .if_match(got.e_tag().unwrap())
        .distribution_config(edited)
        .send()
        .await
        .unwrap();
    let vc = updated
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap()
        .viewer_certificate()
        .unwrap();
    assert_eq!(vc.cloud_front_default_certificate(), Some(false));
    assert_eq!(vc.certificate(), Some(arn));
    assert_eq!(vc.certificate_source().map(|s| s.as_str()), Some("acm"));
}

/// The certificate as read back, with only the ACM ARN and policy edited.
#[allow(deprecated)]
fn switched_to_acm(read: &ViewerCertificate, arn: &str) -> ViewerCertificate {
    ViewerCertificate::builder()
        .set_cloud_front_default_certificate(read.cloud_front_default_certificate())
        .set_certificate(read.certificate().map(str::to_string))
        .set_certificate_source(read.certificate_source().cloned())
        .acm_certificate_arn(arn)
        .ssl_support_method(SslSupportMethod::SniOnly)
        .minimum_protocol_version(MinimumProtocolVersion::TlSv122021)
        .build()
}

#[tokio::test]
async fn cloudfront_web_acl_operations_honor_if_match() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(bare_config("e2e-acl-if-match"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let acl = "arn:aws:wafv2:us-east-1:123456789012:global/webacl/e/abc";

    let err = cf
        .associate_distribution_web_acl()
        .id(&id)
        .web_acl_arn(acl)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidIfMatchVersion")
    );
    let err = cf
        .associate_distribution_web_acl()
        .id(&id)
        .web_acl_arn(acl)
        .if_match("stale")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("PreconditionFailed")
    );
    let associated = cf
        .associate_distribution_web_acl()
        .id(&id)
        .web_acl_arn(acl)
        .if_match(create.e_tag().unwrap())
        .send()
        .await
        .unwrap();
    let etag = associated.e_tag().expect("new ETag").to_string();
    assert_ne!(Some(etag.as_str()), create.e_tag());

    let err = cf
        .disassociate_distribution_web_acl()
        .id(&id)
        .if_match(create.e_tag().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("PreconditionFailed")
    );
    cf.disassociate_distribution_web_acl()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn cloudfront_enabled_logging_without_a_bucket_is_rejected() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let config = bare_builder("e2e-logging")
        .logging(LoggingConfig::builder().enabled(true).build())
        .build()
        .unwrap();
    let err = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidArgument")
    );
}

#[tokio::test]
async fn cloudfront_explicit_staging_false_is_a_no_op_update() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(bare_config("e2e-staging"))
        .send()
        .await
        .unwrap();
    let dist = create.distribution().unwrap();
    assert_eq!(dist.distribution_config().unwrap().staging(), Some(false));

    let updated = cf
        .update_distribution()
        .id(dist.id())
        .if_match(create.e_tag().unwrap())
        .distribution_config(bare_builder("e2e-staging").staging(false).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(updated.e_tag(), create.e_tag());
}

#[tokio::test]
async fn cloudfront_copy_distribution_honors_the_staging_header() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(bare_config("e2e-copy-primary"))
        .send()
        .await
        .unwrap();
    let primary = create.distribution().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();

    let staging = cf
        .copy_distribution()
        .primary_distribution_id(&primary)
        .if_match(&etag)
        .caller_reference("e2e-copy-staging")
        .staging(true)
        .send()
        .await
        .unwrap();
    let staging = staging.distribution().unwrap();
    assert_eq!(staging.distribution_config().unwrap().staging(), Some(true));

    // The flag is stored, not just echoed.
    let read = cf
        .get_distribution_config()
        .id(staging.id())
        .send()
        .await
        .unwrap();
    assert_eq!(read.distribution_config().unwrap().staging(), Some(true));

    let list = cf.list_distributions().send().await.unwrap();
    let items = list.distribution_list().unwrap().items();
    let summary_staging = |id: &str| items.iter().find(|d| d.id() == id).unwrap().staging();
    assert!(summary_staging(staging.id()));
    assert!(!summary_staging(&primary));
}

/// Values recorded from real AWS `CreateDistribution` on 2026-11-09.
#[tokio::test]
async fn cloudfront_omitted_ttls_follow_the_supplied_bounds() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    const DAY: i64 = 86_400;
    const YEAR: i64 = 31_536_000;
    const LONG: i64 = 40_000_000;
    for (caller_ref, min, default, max, expected) in [
        ("e2e-ttl-min0", 0, None, None, (0, DAY, YEAR)),
        // The old inversion: MaxTTL 600 used to leave DefaultTTL at a day.
        ("e2e-ttl-max600", 0, None, Some(600), (0, 600, 600)),
        ("e2e-ttl-def600", 0, Some(600), None, (0, 600, YEAR)),
        ("e2e-ttl-def90k", 0, Some(90_000), None, (0, 90_000, YEAR)),
        ("e2e-ttl-deflong", 0, Some(LONG), None, (0, LONG, LONG)),
        (
            "e2e-ttl-minhigh",
            100_000,
            None,
            None,
            (100_000, 100_000, YEAR),
        ),
        ("e2e-ttl-minlong", LONG, None, None, (LONG, LONG, LONG)),
    ] {
        let mut config = minimal_config(caller_ref);
        let behavior = config.default_cache_behavior.as_mut().unwrap();
        behavior.min_ttl = Some(min);
        behavior.default_ttl = default;
        behavior.max_ttl = max;
        let create = cf
            .create_distribution()
            .distribution_config(config)
            .send()
            .await
            .unwrap();
        let read = cf
            .get_distribution_config()
            .id(create.distribution().unwrap().id())
            .send()
            .await
            .unwrap();
        let dcb = read
            .distribution_config()
            .unwrap()
            .default_cache_behavior()
            .unwrap();
        assert_eq!(
            (dcb.min_ttl(), dcb.default_ttl(), dcb.max_ttl()),
            (Some(expected.0), Some(expected.1), Some(expected.2)),
            "{caller_ref}"
        );
    }
}

const ORDER_MESSAGE: &str = "MinTTL, MaxTTL and DefaultTTL should follow order MinTTL less than \
                             or equal to DefaultTTL less than or equal to MaxTTL";
const MIN_REQUIRED_MESSAGE: &str = "The parameter MinTTL is required.";
const MANAGED_CACHE_POLICY: &str = "658327ea-f89d-4fab-a63d-7e88639e58f6";

/// Applied to a config's `DefaultCacheBehavior`.
type Settings = fn(&mut DefaultCacheBehavior);

fn clear_ttls(b: &mut DefaultCacheBehavior) {
    (b.min_ttl, b.default_ttl, b.max_ttl) = (None, None, None);
}

fn on_policy(b: &mut DefaultCacheBehavior) {
    clear_ttls(b);
    b.forwarded_values = None;
    b.cache_policy_id = Some(MANAGED_CACHE_POLICY.into());
}

fn with_policy_message(member: &str) -> String {
    format!(
        "The parameter {member} cannot be used when a cache policy is associated to the cache behavior."
    )
}

/// Codes and messages recorded from real AWS `CreateDistribution` on 2026-11-09.
fn refused_on_create() -> Vec<(&'static str, Settings, &'static str, String)> {
    let invalid = "InvalidArgument";
    let order = "InvalidTTLOrder";
    vec![
        ("no-ttls", clear_ttls, invalid, MIN_REQUIRED_MESSAGE.into()),
        (
            "max-only",
            |b| {
                clear_ttls(b);
                b.max_ttl = Some(600);
            },
            invalid,
            MIN_REQUIRED_MESSAGE.into(),
        ),
        (
            "neither",
            |b| {
                clear_ttls(b);
                b.forwarded_values = None;
            },
            invalid,
            MIN_REQUIRED_MESSAGE.into(),
        ),
        (
            "min-without-forwarded-values",
            |b| {
                clear_ttls(b);
                b.forwarded_values = None;
                b.min_ttl = Some(0);
            },
            invalid,
            "The parameter ForwardedValues is required.".into(),
        ),
        (
            "min-above-default",
            |b| (b.min_ttl, b.default_ttl, b.max_ttl) = (Some(700), Some(600), Some(800)),
            order,
            ORDER_MESSAGE.into(),
        ),
        (
            "default-above-max",
            |b| (b.min_ttl, b.default_ttl, b.max_ttl) = (Some(0), Some(900), Some(800)),
            order,
            ORDER_MESSAGE.into(),
        ),
        (
            "min-above-max",
            |b| (b.min_ttl, b.default_ttl, b.max_ttl) = (Some(700), None, Some(600)),
            order,
            "MinTTL should be less than or equal to MaxTTL".into(),
        ),
        (
            "min-above-default-no-max",
            |b| (b.min_ttl, b.default_ttl, b.max_ttl) = (Some(700), Some(600), None),
            order,
            "MinTTL should be less than or equal to DefaultTTL".into(),
        ),
        (
            "policy-with-min-out-of-order",
            |b| {
                on_policy(b);
                (b.min_ttl, b.max_ttl) = (Some(700), Some(600));
            },
            invalid,
            with_policy_message("MinTTL"),
        ),
        (
            "policy-with-default",
            |b| {
                on_policy(b);
                b.default_ttl = Some(600);
            },
            invalid,
            with_policy_message("DefaultTTL"),
        ),
        (
            "policy-with-max",
            |b| {
                on_policy(b);
                b.max_ttl = Some(600);
            },
            invalid,
            with_policy_message("MaxTTL"),
        ),
        (
            "policy-with-forwarded-values",
            |b| {
                let forwarded = b.forwarded_values.take();
                on_policy(b);
                b.forwarded_values = forwarded;
            },
            invalid,
            with_policy_message("ForwardedValues"),
        ),
    ]
}

/// `InvalidTTLOrder` and `InvalidArgument` are modeled on these operations, so
/// the SDK must surface them as the typed errors.
#[tokio::test]
async fn cloudfront_create_refuses_invalid_cache_settings() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    for (name, settings, code, message) in refused_on_create() {
        let mut config = minimal_config(&format!("e2e-refuse-{name}"));
        settings(config.default_cache_behavior.as_mut().unwrap());
        let message = Some(message.as_str());

        let err = cf
            .create_distribution()
            .distribution_config(config.clone())
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!((err.code(), err.message()), (Some(code), message), "{name}");
        assert_eq!(
            err.is_invalid_ttl_order(),
            code == "InvalidTTLOrder",
            "{name}"
        );
        assert_eq!(
            err.is_invalid_argument(),
            code == "InvalidArgument",
            "{name}"
        );

        let err = cf
            .create_distribution_with_tags()
            .distribution_config_with_tags(
                DistributionConfigWithTags::builder()
                    .distribution_config(config)
                    .tags(Tags::builder().build())
                    .build(),
            )
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(
            (err.code(), err.message()),
            (Some(code), message),
            "{name} tags"
        );
    }
}

/// Recorded from real AWS `UpdateDistribution` on 2026-11-09. Unlike a create,
/// every TTL is required and they need not be in order.
#[tokio::test]
async fn cloudfront_update_requires_every_ttl_but_not_their_order() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-update-rules"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();

    let refused: [(Settings, String); 5] = [
        (
            |b| b.default_ttl = None,
            "The parameter DefaultTTL is required.".into(),
        ),
        (
            |b| b.max_ttl = None,
            "The parameter MaxTTL is required.".into(),
        ),
        (|b| b.min_ttl = None, MIN_REQUIRED_MESSAGE.into()),
        (
            |b| {
                b.default_ttl = None;
                b.max_ttl = Some(600);
            },
            "The parameter DefaultTTL is required.".into(),
        ),
        (
            |b| b.cache_policy_id = Some(MANAGED_CACHE_POLICY.into()),
            with_policy_message("MinTTL"),
        ),
    ];
    for (settings, message) in refused {
        let mut config = minimal_config("e2e-update-rules");
        settings(config.default_cache_behavior.as_mut().unwrap());
        let err = cf
            .update_distribution()
            .id(&id)
            .if_match(&etag)
            .distribution_config(config)
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(
            (err.code(), err.message()),
            (Some("InvalidArgument"), Some(message.as_str()))
        );
    }

    // The refused updates left the distribution as it was.
    let read = cf.get_distribution_config().id(&id).send().await.unwrap();
    assert_eq!(read.e_tag(), Some(etag.as_str()));

    let mut if_match = etag;
    for ttls in [(700, 600, 800), (700, 600, 600)] {
        let mut config = minimal_config("e2e-update-rules");
        let b = config.default_cache_behavior.as_mut().unwrap();
        (b.min_ttl, b.default_ttl, b.max_ttl) = (Some(ttls.0), Some(ttls.1), Some(ttls.2));
        let updated = cf
            .update_distribution()
            .id(&id)
            .if_match(&if_match)
            .distribution_config(config)
            .send()
            .await
            .unwrap();
        let dcb = updated
            .distribution()
            .unwrap()
            .distribution_config()
            .unwrap()
            .default_cache_behavior()
            .unwrap();
        assert_eq!(
            (dcb.min_ttl(), dcb.default_ttl(), dcb.max_ttl()),
            (Some(ttls.0), Some(ttls.1), Some(ttls.2))
        );
        if_match = updated.e_tag().unwrap().to_string();
    }

    // With the TTLs left out of order, the staging copy is what gets refused.
    let err = cf
        .copy_distribution()
        .primary_distribution_id(&id)
        .if_match(&if_match)
        .caller_reference("e2e-update-rules-copy")
        .staging(true)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.code(), Some("InvalidTTLOrder"));
    assert!(
        err.message()
            .is_some_and(|m| m.starts_with("We couldn\u{2019}t create the staging distribution.")),
        "{err:?}"
    );
}

/// Recorded from real AWS on 2026-11-09.
#[tokio::test]
async fn cloudfront_update_cannot_change_the_mode_and_checks_if_match_first() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let tenant = cf
        .create_distribution()
        .distribution_config(tenant_only_config("e2e-flip-tenant"))
        .send()
        .await
        .unwrap();
    let tenant_id = tenant.distribution().unwrap().id().to_string();
    let read = cf
        .get_distribution_config()
        .id(&tenant_id)
        .send()
        .await
        .unwrap();
    let mut dropped = read.distribution_config().unwrap().clone();
    dropped.connection_mode = None;
    let err = cf
        .update_distribution()
        .id(&tenant_id)
        .if_match(read.e_tag().unwrap())
        .distribution_config(dropped)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_illegal_update(), "{err:?}");
    assert_eq!(
        err.message(),
        Some("Price class is missing for the resource")
    );

    let standard = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-flip-standard"))
        .send()
        .await
        .unwrap();
    let standard_id = standard.distribution().unwrap().id().to_string();
    let read = cf
        .get_distribution_config()
        .id(&standard_id)
        .send()
        .await
        .unwrap();
    let mut flipped = read.distribution_config().unwrap().clone();
    flipped.connection_mode = Some(ConnectionMode::TenantOnly);
    let err = cf
        .update_distribution()
        .id(&standard_id)
        .if_match(read.e_tag().unwrap())
        .distribution_config(flipped.clone())
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(
        err.message(),
        Some("You can't specify the IsIPV6Enabled field for a multi-tenant distribution.")
    );

    // A wrong If-Match wins over a config that would be refused.
    let err = cf
        .update_distribution()
        .id(&standard_id)
        .if_match("E2STALEETAG")
        .distribution_config(flipped.clone())
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_if_match_version(), "{err:?}");
    assert_eq!(
        err.message(),
        Some("The If-Match version is missing or not valid for the resource.")
    );

    let err = cf
        .update_distribution()
        .id("E000000000000")
        .if_match("E2X")
        .distribution_config(flipped)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_no_such_distribution(), "{err:?}");
    assert_eq!(err.message(), Some("Id format is invalid"));
}

/// Recorded from real AWS on 2026-11-09.
#[tokio::test]
async fn cloudfront_if_match_superseded_and_malformed_differ() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-stale"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let first_etag = create.e_tag().unwrap().to_string();
    let mut changed = minimal_config("e2e-stale");
    changed.comment = "changed".to_string();
    cf.update_distribution()
        .id(&id)
        .if_match(&first_etag)
        .distribution_config(changed.clone())
        .send()
        .await
        .unwrap();
    let precondition = "The request failed because it didn't meet the preconditions in one or \
                        more request-header fields.";

    let err = cf
        .update_distribution()
        .id(&id)
        .if_match(&first_etag)
        .distribution_config(minimal_config("e2e-stale"))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_precondition_failed(), "{err:?}");
    assert_eq!(err.message(), Some(precondition));

    // A superseded ETag doesn't outrank an invalid config.
    let mut invalid = minimal_config("e2e-stale");
    invalid.default_cache_behavior.as_mut().unwrap().min_ttl = None;
    let err = cf
        .update_distribution()
        .id(&id)
        .if_match(&first_etag)
        .distribution_config(invalid.clone())
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.message(), Some(MIN_REQUIRED_MESSAGE));

    // A malformed one does.
    let err = cf
        .update_distribution()
        .id(&id)
        .if_match("garbage")
        .distribution_config(invalid)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_if_match_version(), "{err:?}");

    let err = cf
        .delete_distribution()
        .id(&id)
        .if_match(&first_etag)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_precondition_failed(), "{err:?}");
    assert_eq!(err.message(), Some(precondition));
    let err = cf
        .delete_distribution()
        .id(&id)
        .if_match("garbage")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_invalid_if_match_version(), "{err:?}");
    assert_eq!(
        err.message(),
        Some("The If-Match version is missing or not valid for the resource.")
    );

    for (unknown, message) in [
        (
            "EABCDEFGHIJKLM",
            "The specified distribution does not exist.",
        ),
        ("E000000000000", "Id format is invalid"),
    ] {
        let err = cf
            .get_distribution()
            .id(unknown)
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert!(err.is_no_such_distribution(), "{err:?}");
        assert_eq!(err.message(), Some(message));
    }
}

/// Recorded from real AWS on 2026-11-09.
#[tokio::test]
async fn cloudfront_negative_ttls_and_explicit_direct_mode() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let mut negative = minimal_config("e2e-negative");
    let b = negative.default_cache_behavior.as_mut().unwrap();
    (b.min_ttl, b.default_ttl, b.max_ttl) = (Some(-5), Some(-1), Some(0));
    let err = cf
        .create_distribution()
        .distribution_config(negative)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(
        (err.code(), err.message()),
        (
            Some("InvalidArgument"),
            Some("The parameter MinTTL is not within allowed range.")
        )
    );

    let mut direct = minimal_config("e2e-direct-mode");
    direct.connection_mode = Some(ConnectionMode::Direct);
    let create = cf
        .create_distribution()
        .distribution_config(direct)
        .send()
        .await
        .unwrap();
    let config = create
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap();
    assert_eq!(config.connection_mode(), None);
}

/// Recorded from real AWS `CopyDistribution` on 2026-11-09.
#[tokio::test]
async fn cloudfront_copy_refusals_are_as_recorded() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-copy-rules"))
        .send()
        .await
        .unwrap();
    let err = cf
        .copy_distribution()
        .primary_distribution_id(create.distribution().unwrap().id())
        .if_match(create.e_tag().unwrap())
        .caller_reference("e2e-copy-rules-plain")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.code(), Some("InvalidArgument"));
    assert_eq!(
        err.message(),
        Some("Your request must specify Staging as true.")
    );

    let tenant = cf
        .create_distribution()
        .distribution_config(tenant_only_config("e2e-copy-rules-tenant"))
        .send()
        .await
        .unwrap();
    let err = cf
        .copy_distribution()
        .primary_distribution_id(tenant.distribution().unwrap().id())
        .if_match(tenant.e_tag().unwrap())
        .caller_reference("e2e-copy-rules-tenant-copy")
        .staging(true)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.code(), Some("InvalidArgument"));
    assert_eq!(
        err.message(),
        Some("You can't enable continuous deployment for a multi-tenant distribution.")
    );
}

/// Recorded from real AWS `CreateDistribution` on 2026-11-09: a `CacheBehaviors`
/// entry meets the same checks as the default behavior.
#[tokio::test]
async fn cloudfront_checks_each_cache_behavior_like_the_default_one() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let with_behavior = |caller_ref: &str, min, max| {
        let mut config = minimal_config(caller_ref);
        let dcb = config.default_cache_behavior.clone().unwrap();
        config.cache_behaviors = Some(
            CacheBehaviors::builder()
                .quantity(1)
                .items(
                    CacheBehavior::builder()
                        .path_pattern("/api/*")
                        .target_origin_id("primary")
                        .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                        .set_forwarded_values(dcb.forwarded_values)
                        .set_min_ttl(min)
                        .set_max_ttl(max)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        );
        config
    };

    let err = cf
        .create_distribution()
        .distribution_config(with_behavior("e2e-cb-required", None, None))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.message(), Some(MIN_REQUIRED_MESSAGE));

    let err = cf
        .create_distribution()
        .distribution_config(with_behavior("e2e-cb-order", Some(700), Some(600)))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.code(), Some("InvalidTTLOrder"));

    let create = cf
        .create_distribution()
        .distribution_config(with_behavior("e2e-cb-ok", Some(0), Some(600)))
        .send()
        .await
        .unwrap();
    let config = create
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap();
    let behavior = &config.cache_behaviors().unwrap().items()[0];
    assert_eq!(
        (
            behavior.min_ttl(),
            behavior.default_ttl(),
            behavior.max_ttl()
        ),
        (Some(0), Some(600), Some(600))
    );
}

/// A cache policy alone is accepted, and its behavior carries no TTLs (recorded
/// from real AWS on 2026-11-09).
#[tokio::test]
async fn cloudfront_a_cache_policy_behavior_has_no_ttls() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let mut config = minimal_config("e2e-policy-only");
    let behavior = config.default_cache_behavior.as_mut().unwrap();
    on_policy(behavior);
    let create = cf
        .create_distribution()
        .distribution_config(config)
        .send()
        .await
        .unwrap();
    let dcb = create
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap()
        .default_cache_behavior()
        .unwrap();
    assert_eq!(
        (dcb.min_ttl(), dcb.default_ttl(), dcb.max_ttl()),
        (None, None, None)
    );
}

fn tenant_only_config(caller_ref: &str) -> DistributionConfig {
    let mut config = minimal_config(caller_ref);
    config.connection_mode = Some(ConnectionMode::TenantOnly);
    let behavior = config.default_cache_behavior.as_mut().unwrap();
    on_policy(behavior);
    config
}

/// Recorded from real AWS on 2026-11-09.
#[tokio::test]
async fn cloudfront_multi_tenant_distributions_behave_as_recorded() {
    use aws_sdk_cloudfront::error::ProvideErrorMetadata;

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let mut with_ipv6 = tenant_only_config("e2e-mt-ipv6");
    with_ipv6.is_ipv6_enabled = Some(false);
    let mut with_staging = tenant_only_config("e2e-mt-staging");
    with_staging.staging = Some(false);
    let mut with_price_class = tenant_only_config("e2e-mt-price");
    with_price_class.price_class = Some(PriceClass::PriceClass100);
    let mut with_aliases = tenant_only_config("e2e-mt-aliases");
    with_aliases.aliases = Some(Aliases::builder().quantity(0).build().unwrap());
    let mut with_policy_id = tenant_only_config("e2e-mt-cdp");
    with_policy_id.continuous_deployment_policy_id = Some(String::new());
    let mut with_anycast = tenant_only_config("e2e-mt-anycast");
    with_anycast.anycast_ip_list_id = Some(String::new());
    let mut with_both = tenant_only_config("e2e-mt-both");
    with_both.is_ipv6_enabled = Some(false);
    with_both.staging = Some(false);
    let behavior = |caller_ref: &str, set: Settings| {
        let mut config = tenant_only_config(caller_ref);
        set(config.default_cache_behavior.as_mut().unwrap());
        config
    };
    for (config, member) in [
        (with_ipv6, "IsIPV6Enabled"),
        (with_staging, "Staging"),
        (with_price_class, "PriceClass"),
        (with_aliases, "Aliases"),
        (with_policy_id, "ContinuousDeploymentPolicyId"),
        (with_anycast, "AnycastIpListId"),
        (with_both, "IsIPV6Enabled"),
        (
            behavior("e2e-mt-smooth", |b| b.smooth_streaming = Some(false)),
            "SmoothStreaming",
        ),
        (behavior("e2e-mt-min", |b| b.min_ttl = Some(0)), "MinTTL"),
        (
            behavior("e2e-mt-def", |b| b.default_ttl = Some(60)),
            "DefaultTTL",
        ),
        (behavior("e2e-mt-max", |b| b.max_ttl = Some(60)), "MaxTTL"),
        (
            behavior("e2e-mt-fv", |b| {
                b.cache_policy_id = None;
                b.forwarded_values = Some(
                    ForwardedValues::builder()
                        .query_string(false)
                        .cookies(
                            CookiePreference::builder()
                                .forward(ItemSelection::None)
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                );
            }),
            "ForwardedValues",
        ),
    ] {
        let err = cf
            .create_distribution()
            .distribution_config(config)
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(err.code(), Some("InvalidArgument"));
        assert_eq!(
            err.message(),
            Some(
                format!("You can't specify the {member} field for a multi-tenant distribution.")
                    .as_str()
            )
        );
    }

    let err = cf
        .create_distribution()
        .distribution_config(behavior("e2e-mt-neither", |b| b.cache_policy_id = None))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(
        err.message(),
        Some("The CachePolicyId field is required for a multi-tenant distribution.")
    );

    let mut with_http_version = tenant_only_config("e2e-mt-http");
    with_http_version.http_version = Some(HttpVersion::Http2);
    let create = cf
        .create_distribution()
        .distribution_config(with_http_version)
        .send()
        .await
        .unwrap();
    let dist = create.distribution().unwrap();
    let id = dist.id().to_string();
    let config = dist.distribution_config().unwrap();
    assert_eq!(config.default_root_object(), Some(""));
    assert_eq!(config.web_acl_id(), Some(""));
    assert_eq!(config.http_version(), Some(&HttpVersion::Http2));
    assert_eq!(config.connection_mode(), Some(&ConnectionMode::TenantOnly));
    assert_eq!(config.price_class(), None);
    assert_eq!(config.is_ipv6_enabled(), None);
    assert_eq!(config.staging(), None);
    assert_eq!(config.continuous_deployment_policy_id(), None);
    assert!(config.aliases().is_none());
    assert_eq!(
        config.viewer_certificate().unwrap().ssl_support_method(),
        Some(&SslSupportMethod::SniOnly)
    );
    let dcb = config.default_cache_behavior().unwrap();
    assert_eq!(dcb.smooth_streaming(), None);
    assert!(dcb.trusted_signers().is_none());

    // Read back and put unchanged, it is a no-op rather than a refusal.
    let read = cf.get_distribution_config().id(&id).send().await.unwrap();
    let updated = cf
        .update_distribution()
        .id(&id)
        .if_match(read.e_tag().unwrap())
        .distribution_config(read.distribution_config().unwrap().clone())
        .send()
        .await
        .unwrap();
    assert_eq!(updated.e_tag(), read.e_tag());

    let list = cf.list_distributions().send().await.unwrap();
    let summary = &list.distribution_list().unwrap().items()[0];
    assert_eq!(summary.aliases().unwrap().quantity(), 0);
    assert!(!summary.is_ipv6_enabled());
    assert!(!summary.staging());
    assert_eq!(summary.http_version().as_str(), "HTTP2");
    assert_eq!(summary.price_class().as_str(), "PriceClass_All");
    assert_eq!(summary.web_acl_id(), "");
    assert_eq!(summary.connection_mode(), Some(&ConnectionMode::TenantOnly));
    assert!(summary.e_tag().is_some_and(|e| !e.is_empty()));
}

#[tokio::test]
async fn cloudfront_a_standard_distribution_lists_as_direct() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("e2e-direct"))
        .send()
        .await
        .unwrap();
    let dist = create.distribution().unwrap();
    assert_eq!(dist.distribution_config().unwrap().connection_mode(), None);
    assert_eq!(
        dist.distribution_config()
            .unwrap()
            .continuous_deployment_policy_id(),
        Some("")
    );
    cf.create_distribution()
        .distribution_config(tenant_only_config("e2e-direct-tenant"))
        .send()
        .await
        .unwrap();

    let list = cf.list_distributions().send().await.unwrap();
    let list = list.distribution_list().unwrap();
    let summary = list.items().iter().find(|d| d.id() == dist.id()).unwrap();
    assert_eq!(summary.connection_mode(), Some(&ConnectionMode::Direct));

    let by_mode = cf
        .list_distributions_by_connection_mode()
        .connection_mode(ConnectionMode::Direct)
        .send()
        .await
        .unwrap();
    let by_mode = by_mode.distribution_list().unwrap();
    assert_eq!(by_mode.quantity(), 1);
    assert_eq!(by_mode.items()[0].id(), dist.id());
}
