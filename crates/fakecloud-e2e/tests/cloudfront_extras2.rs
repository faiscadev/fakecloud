//! CloudFront Batch 6b E2E: Connection Groups, Domain ops,
//! Managed Certificate Details, UpdateDistributionWithStagingConfig.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    CookiePreference, DefaultCacheBehavior, DistributionConfig, DistributionResourceId,
    ForwardedValues, Headers, ItemSelection, Origin, Origins, ViewerProtocolPolicy,
};
use helpers::TestServer;

fn minimal_config(caller_ref: &str) -> DistributionConfig {
    minimal_config_with_comment(caller_ref, "batch6b")
}

fn minimal_config_with_comment(caller_ref: &str, comment: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment(comment)
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
async fn connection_group_lifecycle() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let create = cf
        .create_connection_group()
        .name("e2e-cg")
        .ipv6_enabled(true)
        .enabled(true)
        .send()
        .await
        .expect("create");
    let g = create.connection_group().expect("cg");
    let id = g.id().expect("id").to_string();
    let routing_endpoint = g.routing_endpoint().expect("re").to_string();
    let etag = create.e_tag().expect("etag").to_string();
    assert!(routing_endpoint.ends_with(".cloudfront.net"));

    let got = cf
        .get_connection_group()
        .identifier(&id)
        .send()
        .await
        .expect("get");
    assert_eq!(got.connection_group().unwrap().id().unwrap(), id);

    let by_re = cf
        .get_connection_group_by_routing_endpoint()
        .routing_endpoint(&routing_endpoint)
        .send()
        .await
        .expect("get by routing endpoint");
    assert_eq!(by_re.connection_group().unwrap().id().unwrap(), id);

    let upd = cf
        .update_connection_group()
        .id(&id)
        .if_match(&etag)
        .enabled(false)
        .send()
        .await
        .expect("update");
    let new_etag = upd.e_tag().unwrap().to_string();

    let list = cf.list_connection_groups().send().await.expect("list");
    assert!(!list.connection_groups.unwrap_or_default().is_empty());

    cf.delete_connection_group()
        .id(&id)
        .if_match(&new_etag)
        .send()
        .await
        .expect("delete");
}

#[tokio::test]
async fn list_domain_conflicts_returns_empty() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let resp = cf
        .list_domain_conflicts()
        .domain("example.com")
        .domain_control_validation_resource(
            DistributionResourceId::builder()
                .distribution_id("E1234")
                .build(),
        )
        .send()
        .await
        .expect("list");
    assert!(resp.domain_conflicts.unwrap_or_default().is_empty());
}

#[tokio::test]
async fn update_domain_association_round_trip() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let resp = cf
        .update_domain_association()
        .domain("docs.example.com")
        .target_resource(
            DistributionResourceId::builder()
                .distribution_id("E1234")
                .build(),
        )
        .send()
        .await
        .expect("update");
    assert_eq!(resp.domain().unwrap(), "docs.example.com");
    assert_eq!(resp.resource_id().unwrap(), "E1234");
}

#[tokio::test]
async fn verify_dns_configuration_returns_status() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let resp = cf
        .verify_dns_configuration()
        .identifier("CG123")
        .domain("docs.example.com")
        .send()
        .await
        .expect("verify");
    let list = resp.dns_configuration_list();
    assert!(!list.is_empty());
    assert_eq!(list[0].domain(), "docs.example.com");
}

#[tokio::test]
async fn get_managed_certificate_details_returns_cert() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let resp = cf
        .get_managed_certificate_details()
        .identifier("CG123")
        .send()
        .await
        .expect("get");
    let details = resp.managed_certificate_details().expect("details");
    assert!(details
        .certificate_arn()
        .unwrap()
        .starts_with("arn:aws:acm:"));
}

#[tokio::test]
async fn update_distribution_with_staging_config_swaps_etag() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let prod = cf
        .create_distribution()
        .distribution_config(minimal_config("prod"))
        .send()
        .await
        .expect("create prod");
    let prod_id = prod.distribution().unwrap().id().to_string();
    let prod_etag = prod.e_tag().unwrap().to_string();

    // Give the staging distribution a distinct config (different comment) so
    // we can prove the promotion actually copies it onto prod.
    let staging = cf
        .create_distribution()
        .distribution_config(minimal_config_with_comment(
            "staging",
            "promoted-staging-config",
        ))
        .send()
        .await
        .expect("create staging");
    let staging_id = staging.distribution().unwrap().id().to_string();

    let resp = cf
        .update_distribution_with_staging_config()
        .id(&prod_id)
        .staging_distribution_id(&staging_id)
        .if_match(&prod_etag)
        .send()
        .await
        .expect("promote staging");
    assert!(resp.e_tag().unwrap() != prod_etag);

    // Promotion must copy the staging config onto prod (bug-hunt 2026-06-24,
    // 1.13 — previously only the ETag changed). The promoted prod is no longer
    // a staging distribution.
    let got = cf
        .get_distribution()
        .id(&prod_id)
        .send()
        .await
        .expect("get prod");
    let cfg = got.distribution().unwrap().distribution_config().unwrap();
    assert_eq!(cfg.comment(), "promoted-staging-config");
    assert_eq!(cfg.staging(), Some(false));
}
