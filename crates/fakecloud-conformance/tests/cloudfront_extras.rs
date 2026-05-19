//! CloudFront Batch 6a conformance tests: VPC Origins, Anycast IP Lists,
//! Trust Stores, Resource Policies.

mod helpers;

use aws_sdk_cloudfront::types::{
    CaCertificatesBundleS3Location, CaCertificatesBundleSource, IpAddressType,
    OriginProtocolPolicy, OriginSslProtocols, SslProtocol, VpcOriginEndpointConfig,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

fn vpc_endpoint_cfg(name: &str) -> VpcOriginEndpointConfig {
    VpcOriginEndpointConfig::builder()
        .name(name)
        .arn("arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/x/y")
        .http_port(80)
        .https_port(443)
        .origin_protocol_policy(OriginProtocolPolicy::HttpsOnly)
        .origin_ssl_protocols(
            OriginSslProtocols::builder()
                .quantity(1)
                .items(SslProtocol::TlSv12)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn trust_store_bundle() -> CaCertificatesBundleSource {
    CaCertificatesBundleSource::CaCertificatesBundleS3Location(
        CaCertificatesBundleS3Location::builder()
            .bucket("ca-bundle")
            .key("certs/bundle.pem")
            .region("us-east-1")
            .build()
            .unwrap(),
    )
}

// ─── VPC Origin ──────────────────────────────────────────────────────

#[test_action("cloudfront", "CreateVpcOrigin", checksum = "ca0b375c")]
#[tokio::test]
async fn cf_create_vpc_origin() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_vpc_origin()
        .vpc_origin_endpoint_config(vpc_endpoint_cfg("conf-vpc-1"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetVpcOrigin", checksum = "cd09e929")]
#[tokio::test]
async fn cf_get_vpc_origin() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_vpc_origin()
        .vpc_origin_endpoint_config(vpc_endpoint_cfg("conf-vpc-2"))
        .send()
        .await
        .unwrap();
    let id = c.vpc_origin().unwrap().id().to_string();
    cf.get_vpc_origin().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "UpdateVpcOrigin", checksum = "c1777131")]
#[tokio::test]
async fn cf_update_vpc_origin() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_vpc_origin()
        .vpc_origin_endpoint_config(vpc_endpoint_cfg("conf-vpc-3"))
        .send()
        .await
        .unwrap();
    let id = c.vpc_origin().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_vpc_origin()
        .id(&id)
        .if_match(&etag)
        .vpc_origin_endpoint_config(vpc_endpoint_cfg("conf-vpc-3"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteVpcOrigin", checksum = "418169cb")]
#[tokio::test]
async fn cf_delete_vpc_origin() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_vpc_origin()
        .vpc_origin_endpoint_config(vpc_endpoint_cfg("conf-vpc-4"))
        .send()
        .await
        .unwrap();
    let id = c.vpc_origin().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_vpc_origin()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListVpcOrigins", checksum = "d12c4719")]
#[tokio::test]
async fn cf_list_vpc_origins() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_vpc_origins().send().await.unwrap();
}

// ─── Anycast IP List ──────────────────────────────────────────────────

#[test_action("cloudfront", "CreateAnycastIpList", checksum = "07015d50")]
#[tokio::test]
async fn cf_create_anycast_ip_list() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_anycast_ip_list()
        .name("conf-aip-1")
        .ip_count(3)
        .ip_address_type(IpAddressType::Ipv4)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetAnycastIpList", checksum = "feb0f91d")]
#[tokio::test]
async fn cf_get_anycast_ip_list() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_anycast_ip_list()
        .name("conf-aip-2")
        .ip_count(3)
        .send()
        .await
        .unwrap();
    let id = c.anycast_ip_list().unwrap().id().to_string();
    cf.get_anycast_ip_list().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "UpdateAnycastIpList", checksum = "353ffc90")]
#[tokio::test]
async fn cf_update_anycast_ip_list() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_anycast_ip_list()
        .name("conf-aip-3")
        .ip_count(3)
        .send()
        .await
        .unwrap();
    let id = c.anycast_ip_list().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_anycast_ip_list()
        .id(&id)
        .if_match(&etag)
        .ip_address_type(IpAddressType::Ipv4)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteAnycastIpList", checksum = "9632bd0e")]
#[tokio::test]
async fn cf_delete_anycast_ip_list() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_anycast_ip_list()
        .name("conf-aip-4")
        .ip_count(3)
        .send()
        .await
        .unwrap();
    let id = c.anycast_ip_list().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_anycast_ip_list()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListAnycastIpLists", checksum = "76540130")]
#[tokio::test]
async fn cf_list_anycast_ip_lists() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_anycast_ip_lists().send().await.unwrap();
}

// ─── Trust Store ──────────────────────────────────────────────────────

#[test_action("cloudfront", "CreateTrustStore", checksum = "f5851e5b")]
#[tokio::test]
async fn cf_create_trust_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_trust_store()
        .name("conf-ts-1")
        .ca_certificates_bundle_source(trust_store_bundle())
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetTrustStore", checksum = "ff3e6c6e")]
#[tokio::test]
async fn cf_get_trust_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_trust_store()
        .name("conf-ts-2")
        .ca_certificates_bundle_source(trust_store_bundle())
        .send()
        .await
        .unwrap();
    let id = c.trust_store().unwrap().id().unwrap().to_string();
    cf.get_trust_store().identifier(&id).send().await.unwrap();
}

#[test_action("cloudfront", "UpdateTrustStore", checksum = "a43ad917")]
#[tokio::test]
async fn cf_update_trust_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_trust_store()
        .name("conf-ts-3")
        .ca_certificates_bundle_source(trust_store_bundle())
        .send()
        .await
        .unwrap();
    let id = c.trust_store().unwrap().id().unwrap().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_trust_store()
        .id(&id)
        .if_match(&etag)
        .ca_certificates_bundle_source(trust_store_bundle())
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteTrustStore", checksum = "c5607c8f")]
#[tokio::test]
async fn cf_delete_trust_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_trust_store()
        .name("conf-ts-4")
        .ca_certificates_bundle_source(trust_store_bundle())
        .send()
        .await
        .unwrap();
    let id = c.trust_store().unwrap().id().unwrap().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_trust_store()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListTrustStores", checksum = "d6b6b996")]
#[tokio::test]
async fn cf_list_trust_stores() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_trust_stores().send().await.unwrap();
}

// ─── Resource Policy ──────────────────────────────────────────────────

#[test_action("cloudfront", "PutResourcePolicy", checksum = "3561ff29")]
#[tokio::test]
async fn cf_put_resource_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.put_resource_policy()
        .resource_arn("arn:aws:cloudfront::000000000000:distribution/E1")
        .policy_document(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetResourcePolicy", checksum = "68683c0f")]
#[tokio::test]
async fn cf_get_resource_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let arn = "arn:aws:cloudfront::000000000000:distribution/E2";
    cf.put_resource_policy()
        .resource_arn(arn)
        .policy_document("{}")
        .send()
        .await
        .unwrap();
    cf.get_resource_policy()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteResourcePolicy", checksum = "b14624d7")]
#[tokio::test]
async fn cf_delete_resource_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let arn = "arn:aws:cloudfront::000000000000:distribution/E3";
    cf.put_resource_policy()
        .resource_arn(arn)
        .policy_document("{}")
        .send()
        .await
        .unwrap();
    cf.delete_resource_policy()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}
