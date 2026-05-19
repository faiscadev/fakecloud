//! CloudFront Batch 1 conformance tests.
//!
//! Each `#[test_action]` pairs a real AWS SDK call with the Smithy shape
//! checksum. If AWS rev-bumps the CloudFront model the checksum goes stale
//! and the build fails loudly so we know to refresh it.

mod helpers;

use aws_sdk_cloudfront::types::{
    DefaultCacheBehavior, DistributionConfig, InvalidationBatch, Origin, Origins, Paths, Tag, Tags,
    ViewerProtocolPolicy,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

fn minimal_config(caller_ref: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("conf")
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
        .build()
        .unwrap()
}

#[test_action("cloudfront", "CreateDistribution", checksum = "bcbc8e42")]
#[tokio::test]
async fn cloudfront_create_distribution() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let resp = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-create"))
        .send()
        .await
        .unwrap();
    let dist = resp.distribution().unwrap();
    assert!(dist.id().starts_with('E'));
    assert!(dist.arn().contains(":distribution/"));
}

#[test_action("cloudfront", "GetDistribution", checksum = "2e5def23")]
#[tokio::test]
async fn cloudfront_get_distribution() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-get"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let resp = cf.get_distribution().id(&id).send().await.unwrap();
    assert_eq!(resp.distribution().unwrap().id(), id);
}

#[test_action("cloudfront", "GetDistributionConfig", checksum = "4299a175")]
#[tokio::test]
async fn cloudfront_get_distribution_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-getcfg"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let resp = cf.get_distribution_config().id(&id).send().await.unwrap();
    assert_eq!(
        resp.distribution_config().unwrap().caller_reference(),
        "conf-getcfg"
    );
}

#[test_action("cloudfront", "UpdateDistribution", checksum = "c007953d")]
#[tokio::test]
async fn cloudfront_update_distribution() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-update"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    let mut new_cfg = minimal_config("conf-update");
    new_cfg = DistributionConfig::builder()
        .caller_reference(new_cfg.caller_reference())
        .comment("rev2")
        .enabled(true)
        .origins(new_cfg.origins().unwrap().clone())
        .default_cache_behavior(new_cfg.default_cache_behavior().unwrap().clone())
        .build()
        .unwrap();
    cf.update_distribution()
        .id(&id)
        .if_match(&etag)
        .distribution_config(new_cfg)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteDistribution", checksum = "3ad2fb4e")]
#[tokio::test]
async fn cloudfront_delete_distribution() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-del"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    let mut new_cfg = minimal_config("conf-del");
    new_cfg = DistributionConfig::builder()
        .caller_reference(new_cfg.caller_reference())
        .comment("disable")
        .enabled(false)
        .origins(new_cfg.origins().unwrap().clone())
        .default_cache_behavior(new_cfg.default_cache_behavior().unwrap().clone())
        .build()
        .unwrap();
    let upd = cf
        .update_distribution()
        .id(&id)
        .if_match(&etag)
        .distribution_config(new_cfg)
        .send()
        .await
        .unwrap();
    let new_etag = upd.e_tag().unwrap().to_string();
    cf.delete_distribution()
        .id(&id)
        .if_match(&new_etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListDistributions", checksum = "7a63818a")]
#[tokio::test]
async fn cloudfront_list_distributions() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_distribution()
        .distribution_config(minimal_config("conf-list"))
        .send()
        .await
        .unwrap();
    let resp = cf.list_distributions().send().await.unwrap();
    assert!(resp.distribution_list().unwrap().quantity() >= 1);
}

#[test_action("cloudfront", "CreateInvalidation", checksum = "36cfb5a0")]
#[tokio::test]
async fn cloudfront_create_invalidation() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-invc"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    cf.create_invalidation()
        .distribution_id(&id)
        .invalidation_batch(
            InvalidationBatch::builder()
                .caller_reference("inv-1")
                .paths(Paths::builder().quantity(1).items("/*").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetInvalidation", checksum = "5d20987d")]
#[tokio::test]
async fn cloudfront_get_invalidation() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-invg"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let inv = cf
        .create_invalidation()
        .distribution_id(&id)
        .invalidation_batch(
            InvalidationBatch::builder()
                .caller_reference("inv-2")
                .paths(Paths::builder().quantity(1).items("/*").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let inv_id = inv.invalidation().unwrap().id().to_string();
    let got = cf
        .get_invalidation()
        .distribution_id(&id)
        .id(&inv_id)
        .send()
        .await
        .unwrap();
    assert_eq!(got.invalidation().unwrap().id(), inv_id);
}

#[test_action("cloudfront", "ListInvalidations", checksum = "d3725f75")]
#[tokio::test]
async fn cloudfront_list_invalidations() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-invl"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    cf.create_invalidation()
        .distribution_id(&id)
        .invalidation_batch(
            InvalidationBatch::builder()
                .caller_reference("inv-3")
                .paths(Paths::builder().quantity(1).items("/*").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let resp = cf
        .list_invalidations()
        .distribution_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.invalidation_list().unwrap().quantity(), 1);
}

#[test_action("cloudfront", "TagResource", checksum = "8aad567d")]
#[tokio::test]
async fn cloudfront_tag_resource() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-tag"))
        .send()
        .await
        .unwrap();
    let arn = create.distribution().unwrap().arn().to_string();
    cf.tag_resource()
        .resource(&arn)
        .tags(
            Tags::builder()
                .items(Tag::builder().key("env").value("conf").build().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UntagResource", checksum = "686d7847")]
#[tokio::test]
async fn cloudfront_untag_resource() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-untag"))
        .send()
        .await
        .unwrap();
    let arn = create.distribution().unwrap().arn().to_string();
    cf.tag_resource()
        .resource(&arn)
        .tags(
            Tags::builder()
                .items(Tag::builder().key("e").value("v").build().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap();
    cf.untag_resource()
        .resource(&arn)
        .tag_keys(
            aws_sdk_cloudfront::types::TagKeys::builder()
                .items("e")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListTagsForResource", checksum = "46847064")]
#[tokio::test]
async fn cloudfront_list_tags_for_resource() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-listtag"))
        .send()
        .await
        .unwrap();
    let arn = create.distribution().unwrap().arn().to_string();
    cf.list_tags_for_resource()
        .resource(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "AssociateAlias", checksum = "7ada3ff5")]
#[tokio::test]
async fn cloudfront_associate_alias() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-alias"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    cf.associate_alias()
        .target_distribution_id(&id)
        .alias("conf-alias.example.com")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListConflictingAliases", checksum = "5d2e1e14")]
#[tokio::test]
async fn cloudfront_list_conflicting_aliases() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-conflict"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    cf.list_conflicting_aliases()
        .distribution_id(&id)
        .alias("foo.example.com")
        .send()
        .await
        .unwrap();
}

// ─── Distribution-by-X listings (gap-fill for audit) ───────────────────

async fn make_dist(server: &TestServer, caller_ref: &str) -> String {
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config(caller_ref))
        .send()
        .await
        .unwrap();
    create.distribution().unwrap().id().to_string()
}

#[test_action("cloudfront", "CreateDistributionWithTags", checksum = "ad37fba0")]
#[tokio::test]
async fn cloudfront_create_distribution_with_tags() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_distribution_with_tags()
        .distribution_config_with_tags(
            aws_sdk_cloudfront::types::DistributionConfigWithTags::builder()
                .distribution_config(minimal_config("conf-cdwt"))
                .tags(
                    Tags::builder()
                        .items(Tag::builder().key("env").value("test").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "CopyDistribution", checksum = "f0499eac")]
#[tokio::test]
async fn cloudfront_copy_distribution() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-copy-src"))
        .send()
        .await
        .unwrap();
    let id = create.distribution().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.copy_distribution()
        .primary_distribution_id(&id)
        .if_match(&etag)
        .caller_reference("conf-copy-1")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "AssociateDistributionWebACL", checksum = "b3ceaafe")]
#[tokio::test]
async fn cloudfront_associate_web_acl() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let id = make_dist(&server, "conf-webacl").await;
    cf.associate_distribution_web_acl()
        .id(&id)
        .web_acl_arn("arn:aws:wafv2:us-east-1:000000000000:global/webacl/conf/abc")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DisassociateDistributionWebACL", checksum = "f95fc84d")]
#[tokio::test]
async fn cloudfront_disassociate_web_acl() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let id = make_dist(&server, "conf-webacl-dis").await;
    cf.disassociate_distribution_web_acl()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByCachePolicyId",
    checksum = "12e1b4fd"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_cache_policy_id() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_cache_policy_id()
        .cache_policy_id("658327ea-f89d-4fab-a63d-7e88639e58f6")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByOriginRequestPolicyId",
    checksum = "90a0f8fa"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_origin_request_policy_id() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_origin_request_policy_id()
        .origin_request_policy_id("88a5eaf4-2fd4-4709-b370-b4c650ea3fcf")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByResponseHeadersPolicyId",
    checksum = "cea636c9"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_response_headers_policy_id() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_response_headers_policy_id()
        .response_headers_policy_id("60669652-455b-4ae9-85a4-c4c02393f86c")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListDistributionsByKeyGroup", checksum = "a1b7c588")]
#[tokio::test]
async fn cloudfront_list_dist_by_key_group() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_key_group()
        .key_group_id("conf-kg")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListDistributionsByWebACLId", checksum = "d461ccad")]
#[tokio::test]
async fn cloudfront_list_dist_by_web_acl_id() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_web_acl_id()
        .web_acl_id("conf-acl")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListDistributionsByVpcOriginId", checksum = "39d538d7")]
#[tokio::test]
async fn cloudfront_list_dist_by_vpc_origin_id() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_vpc_origin_id()
        .vpc_origin_id("conf-vpc")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByAnycastIpListId",
    checksum = "1297d0e2"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_anycast_ip_list_id() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_anycast_ip_list_id()
        .anycast_ip_list_id("conf-anycast")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByConnectionMode",
    checksum = "62257154"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_connection_mode() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_connection_mode()
        .connection_mode(aws_sdk_cloudfront::types::ConnectionMode::Direct)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByOwnedResource",
    checksum = "3c795df1"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_owned_resource() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_owned_resource()
        .resource_arn("arn:aws:wafv2:us-east-1:000000000000:global/webacl/x/y")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListDistributionsByTrustStore", checksum = "326f1fd0")]
#[tokio::test]
async fn cloudfront_list_dist_by_trust_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_trust_store()
        .trust_store_identifier("conf-trust")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByRealtimeLogConfig",
    checksum = "68015f4f"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_realtime_log_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_realtime_log_config()
        .realtime_log_config_arn("arn:aws:cloudfront::000000000000:realtime-log-config/conf-rt")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionsByConnectionFunction",
    checksum = "37d12109"
)]
#[tokio::test]
async fn cloudfront_list_dist_by_connection_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distributions_by_connection_function()
        .connection_function_identifier("conf-cf")
        .send()
        .await
        .unwrap();
}
