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
    CacheBehavior, CacheBehaviors, CachedMethods, CookiePreference, CustomErrorResponse,
    CustomErrorResponses, DefaultCacheBehavior, DistributionConfig, ForwardedValues,
    GeoRestriction, GeoRestrictionType, Headers, HttpVersion, ItemSelection, LoggingConfig, Method,
    MinimumProtocolVersion, Origin, Origins, Paths, PriceClass, Restrictions, SslSupportMethod,
    ViewerCertificate, ViewerProtocolPolicy,
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
    assert!(err.is_precondition_failed(), "{err:?}");
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
    assert_eq!(summary.http_version(), &HttpVersion::Http2);
    assert!(summary.is_ipv6_enabled());
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
