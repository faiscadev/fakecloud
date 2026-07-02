//! CloudFront Batch 6b conformance tests: Connection Groups, Domain ops,
//! Managed Certificate Details, UpdateDistributionWithStagingConfig.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    CookiePreference, DefaultCacheBehavior, DistributionConfig, DistributionResourceId,
    ForwardedValues, Headers, ItemSelection, Origin, Origins, ViewerProtocolPolicy,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

fn minimal_config(caller_ref: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("conf6b")
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

// ─── Connection Group ────────────────────────────────────────────────

#[test_action("cloudfront", "CreateConnectionGroup", checksum = "782b04fe")]
#[tokio::test]
async fn cf_create_connection_group() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_connection_group()
        .name("conf-cg-1")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetConnectionGroup", checksum = "b478c699")]
#[tokio::test]
async fn cf_get_connection_group() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_connection_group()
        .name("conf-cg-2")
        .send()
        .await
        .unwrap();
    let id = c.connection_group().unwrap().id().unwrap().to_string();
    cf.get_connection_group()
        .identifier(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "GetConnectionGroupByRoutingEndpoint",
    checksum = "11d95bf0"
)]
#[tokio::test]
async fn cf_get_connection_group_by_routing_endpoint() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_connection_group()
        .name("conf-cg-3")
        .send()
        .await
        .unwrap();
    let re = c
        .connection_group()
        .unwrap()
        .routing_endpoint()
        .unwrap()
        .to_string();
    cf.get_connection_group_by_routing_endpoint()
        .routing_endpoint(&re)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateConnectionGroup", checksum = "bcf2aca5")]
#[tokio::test]
async fn cf_update_connection_group() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_connection_group()
        .name("conf-cg-4")
        .send()
        .await
        .unwrap();
    let id = c.connection_group().unwrap().id().unwrap().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_connection_group()
        .id(&id)
        .if_match(&etag)
        .enabled(false)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteConnectionGroup", checksum = "1c529024")]
#[tokio::test]
async fn cf_delete_connection_group() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_connection_group()
        .name("conf-cg-5")
        .enabled(false)
        .send()
        .await
        .unwrap();
    let id = c.connection_group().unwrap().id().unwrap().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_connection_group()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListConnectionGroups", checksum = "dbbb2b6b")]
#[tokio::test]
async fn cf_list_connection_groups() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_connection_groups().send().await.unwrap();
}

// ─── Domain ops ──────────────────────────────────────────────────────

#[test_action("cloudfront", "ListDomainConflicts", checksum = "cd7f51a5")]
#[tokio::test]
async fn cf_list_domain_conflicts() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_domain_conflicts()
        .domain("example.com")
        .domain_control_validation_resource(
            DistributionResourceId::builder()
                .distribution_id("E1234")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateDomainAssociation", checksum = "5eac5bbc")]
#[tokio::test]
async fn cf_update_domain_association() {
    use aws_sdk_cloudfront::types::DomainItem;
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    // Seed a real SOURCE tenant that currently owns docs.example.com, so the
    // update exercises the domain-MOVE path (detach from the source + attach to
    // the target), not an unowned-domain attach.
    let source = cf
        .create_distribution_tenant()
        .distribution_id("E1234")
        .name("conf6b-domainassoc-src")
        .domains(
            DomainItem::builder()
                .domain("docs.example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let source_id = source
        .distribution_tenant()
        .unwrap()
        .id()
        .unwrap()
        .to_string();

    // UpdateDomainAssociation validates that the target resource exists (AWS
    // returns EntityNotFound otherwise), so use a real target distribution.
    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("conf6b-domainassoc"))
        .send()
        .await
        .unwrap();
    let target_id = create.distribution().unwrap().id().to_string();

    // Move docs.example.com from the source tenant to the target distribution.
    let resp = cf
        .update_domain_association()
        .domain("docs.example.com")
        .target_resource(
            DistributionResourceId::builder()
                .distribution_id(&target_id)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.resource_id(), Some(target_id.as_str()));

    // (a) The target distribution now owns the domain (as a CNAME alias).
    let target = cf.get_distribution().id(&target_id).send().await.unwrap();
    let aliases = target
        .distribution()
        .unwrap()
        .distribution_config()
        .unwrap()
        .aliases()
        .expect("target should have aliases after the move");
    assert!(
        aliases.items().iter().any(|c| c == "docs.example.com"),
        "target distribution should own the moved domain, got {:?}",
        aliases.items()
    );

    // (b) The source tenant no longer owns the domain (detach happened).
    let src = cf
        .get_distribution_tenant()
        .identifier(&source_id)
        .send()
        .await
        .unwrap();
    let still_owned = src
        .distribution_tenant()
        .unwrap()
        .domains()
        .iter()
        .any(|d| d.domain() == "docs.example.com");
    assert!(
        !still_owned,
        "source tenant should no longer own the moved domain after the move"
    );
}

#[test_action("cloudfront", "VerifyDnsConfiguration", checksum = "5ae578cb")]
#[tokio::test]
async fn cf_verify_dns_configuration() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.verify_dns_configuration()
        .identifier("CG123")
        .domain("docs.example.com")
        .send()
        .await
        .unwrap();
}

// ─── Managed certificate ─────────────────────────────────────────────

#[test_action("cloudfront", "GetManagedCertificateDetails", checksum = "83592b20")]
#[tokio::test]
async fn cf_get_managed_certificate_details() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.get_managed_certificate_details()
        .identifier("CG123")
        .send()
        .await
        .unwrap();
}

// ─── Staging config ──────────────────────────────────────────────────

#[test_action(
    "cloudfront",
    "UpdateDistributionWithStagingConfig",
    checksum = "502897dc"
)]
#[tokio::test]
async fn cf_update_distribution_with_staging_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let prod = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-prod"))
        .send()
        .await
        .unwrap();
    let prod_id = prod.distribution().unwrap().id().to_string();
    let prod_etag = prod.e_tag().unwrap().to_string();
    let staging = cf
        .create_distribution()
        .distribution_config(minimal_config("conf-staging"))
        .send()
        .await
        .unwrap();
    let staging_id = staging.distribution().unwrap().id().to_string();
    cf.update_distribution_with_staging_config()
        .id(&prod_id)
        .staging_distribution_id(&staging_id)
        .if_match(&prod_etag)
        .send()
        .await
        .unwrap();
}

// ─── Distribution Tenants ────────────────────────────────────────────

async fn create_tenant(cf: &aws_sdk_cloudfront::Client, name: &str) -> (String, String) {
    let resp = cf
        .create_distribution_tenant()
        .distribution_id("E1234")
        .name(name)
        .enabled(true)
        .send()
        .await
        .unwrap();
    let id = resp
        .distribution_tenant()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    let etag = resp.e_tag().unwrap().to_string();
    (id, etag)
}

#[test_action("cloudfront", "CreateDistributionTenant", checksum = "5727cf8f")]
#[tokio::test]
async fn cf_create_distribution_tenant() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    create_tenant(&cf, "conf-tenant-1").await;
}

#[test_action("cloudfront", "GetDistributionTenant", checksum = "e5f7687a")]
#[tokio::test]
async fn cf_get_distribution_tenant() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, _) = create_tenant(&cf, "conf-tenant-2").await;
    cf.get_distribution_tenant()
        .identifier(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetDistributionTenantByDomain", checksum = "8d02c083")]
#[tokio::test]
async fn cf_get_distribution_tenant_by_domain() {
    use aws_sdk_cloudfront::types::DomainItem;
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_distribution_tenant()
        .distribution_id("E1234")
        .name("conf-tenant-3")
        .domains(
            DomainItem::builder()
                .domain("docs.example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    cf.get_distribution_tenant_by_domain()
        .domain("docs.example.com")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateDistributionTenant", checksum = "8b0efe18")]
#[tokio::test]
async fn cf_update_distribution_tenant() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, etag) = create_tenant(&cf, "conf-tenant-4").await;
    cf.update_distribution_tenant()
        .id(&id)
        .if_match(&etag)
        .enabled(false)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteDistributionTenant", checksum = "7e831b6b")]
#[tokio::test]
async fn cf_delete_distribution_tenant() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let resp = cf
        .create_distribution_tenant()
        .distribution_id("E1234")
        .name("conf-tenant-5")
        .enabled(false)
        .send()
        .await
        .unwrap();
    let id = resp
        .distribution_tenant()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    let etag = resp.e_tag().unwrap().to_string();
    cf.delete_distribution_tenant()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListDistributionTenants", checksum = "7518eabb")]
#[tokio::test]
async fn cf_list_distribution_tenants() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distribution_tenants().send().await.unwrap();
}

#[test_action(
    "cloudfront",
    "ListDistributionTenantsByCustomization",
    checksum = "178df5d9"
)]
#[tokio::test]
async fn cf_list_distribution_tenants_by_customization() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_distribution_tenants_by_customization()
        .web_acl_arn("arn:aws:wafv2::000000000000:global/webacl/example/abc")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "AssociateDistributionTenantWebACL",
    checksum = "c23f80f5"
)]
#[tokio::test]
async fn cf_associate_distribution_tenant_web_acl() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, etag) = create_tenant(&cf, "conf-tenant-6").await;
    cf.associate_distribution_tenant_web_acl()
        .id(&id)
        .if_match(&etag)
        .web_acl_arn("arn:aws:wafv2::000000000000:global/webacl/example/abc")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "DisassociateDistributionTenantWebACL",
    checksum = "837fc3ab"
)]
#[tokio::test]
async fn cf_disassociate_distribution_tenant_web_acl() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, etag) = create_tenant(&cf, "conf-tenant-7").await;
    let assoc = cf
        .associate_distribution_tenant_web_acl()
        .id(&id)
        .if_match(&etag)
        .web_acl_arn("arn:aws:wafv2::000000000000:global/webacl/example/abc")
        .send()
        .await
        .unwrap();
    let etag2 = assoc.e_tag().unwrap().to_string();
    cf.disassociate_distribution_tenant_web_acl()
        .id(&id)
        .if_match(&etag2)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "CreateInvalidationForDistributionTenant",
    checksum = "f22b2e6d"
)]
#[tokio::test]
async fn cf_create_invalidation_for_distribution_tenant() {
    use aws_sdk_cloudfront::types::{InvalidationBatch, Paths};
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, _) = create_tenant(&cf, "conf-tenant-8").await;
    cf.create_invalidation_for_distribution_tenant()
        .id(&id)
        .invalidation_batch(
            InvalidationBatch::builder()
                .caller_reference("conf-1")
                .paths(Paths::builder().quantity(1).items("/*").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "GetInvalidationForDistributionTenant",
    checksum = "fc7cd4e9"
)]
#[tokio::test]
async fn cf_get_invalidation_for_distribution_tenant() {
    use aws_sdk_cloudfront::types::{InvalidationBatch, Paths};
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, _) = create_tenant(&cf, "conf-tenant-9").await;
    let inv = cf
        .create_invalidation_for_distribution_tenant()
        .id(&id)
        .invalidation_batch(
            InvalidationBatch::builder()
                .caller_reference("conf-2")
                .paths(Paths::builder().quantity(1).items("/*").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let inv_id = inv.invalidation().unwrap().id().to_string();
    cf.get_invalidation_for_distribution_tenant()
        .distribution_tenant_id(&id)
        .id(&inv_id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListInvalidationsForDistributionTenant",
    checksum = "604db2e3"
)]
#[tokio::test]
async fn cf_list_invalidations_for_distribution_tenant() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (id, _) = create_tenant(&cf, "conf-tenant-10").await;
    cf.list_invalidations_for_distribution_tenant()
        .id(&id)
        .send()
        .await
        .unwrap();
}

// ─── Connection Functions ────────────────────────────────────────────

async fn create_cf_func(cf: &aws_sdk_cloudfront::Client, name: &str) -> (String, String) {
    use aws_sdk_cloudfront::primitives::Blob;
    use aws_sdk_cloudfront::types::{FunctionConfig, FunctionRuntime};
    let resp = cf
        .create_connection_function()
        .name(name)
        .connection_function_config(
            FunctionConfig::builder()
                .comment("conf")
                .runtime(FunctionRuntime::CloudfrontJs20)
                .build()
                .unwrap(),
        )
        .connection_function_code(Blob::new(b"function handler() {}".to_vec()))
        .send()
        .await
        .unwrap();
    let etag = resp.e_tag().unwrap().to_string();
    (name.to_string(), etag)
}

#[test_action("cloudfront", "CreateConnectionFunction", checksum = "e7154db7")]
#[tokio::test]
async fn cf_create_connection_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    create_cf_func(&cf, "conf-cfn-1").await;
}

#[test_action("cloudfront", "GetConnectionFunction", checksum = "6546786b")]
#[tokio::test]
async fn cf_get_connection_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (name, _) = create_cf_func(&cf, "conf-cfn-2").await;
    cf.get_connection_function()
        .identifier(&name)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DescribeConnectionFunction", checksum = "75c8c4c9")]
#[tokio::test]
async fn cf_describe_connection_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (name, _) = create_cf_func(&cf, "conf-cfn-3").await;
    cf.describe_connection_function()
        .identifier(&name)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateConnectionFunction", checksum = "224e25ea")]
#[tokio::test]
async fn cf_update_connection_function() {
    use aws_sdk_cloudfront::primitives::Blob;
    use aws_sdk_cloudfront::types::{FunctionConfig, FunctionRuntime};
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (name, etag) = create_cf_func(&cf, "conf-cfn-4").await;
    cf.update_connection_function()
        .id(&name)
        .if_match(&etag)
        .connection_function_config(
            FunctionConfig::builder()
                .comment("updated")
                .runtime(FunctionRuntime::CloudfrontJs20)
                .build()
                .unwrap(),
        )
        .connection_function_code(Blob::new(b"function handler2() {}".to_vec()))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteConnectionFunction", checksum = "c0df295e")]
#[tokio::test]
async fn cf_delete_connection_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (name, etag) = create_cf_func(&cf, "conf-cfn-5").await;
    cf.delete_connection_function()
        .id(&name)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListConnectionFunctions", checksum = "65b39c33")]
#[tokio::test]
async fn cf_list_connection_functions() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_connection_functions().send().await.unwrap();
}

#[test_action("cloudfront", "PublishConnectionFunction", checksum = "ba4441f7")]
#[tokio::test]
async fn cf_publish_connection_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (name, etag) = create_cf_func(&cf, "conf-cfn-6").await;
    cf.publish_connection_function()
        .id(&name)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "TestConnectionFunction", checksum = "f38011e0")]
#[tokio::test]
async fn cf_test_connection_function() {
    use aws_sdk_cloudfront::primitives::Blob;
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let (name, etag) = create_cf_func(&cf, "conf-cfn-7").await;
    cf.test_connection_function()
        .id(&name)
        .if_match(&etag)
        .connection_object(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();
}
