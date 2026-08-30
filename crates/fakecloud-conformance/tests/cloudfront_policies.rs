//! CloudFront Batch 2 conformance tests: OAC + Cache, Origin Request,
//! Response Headers, and Continuous Deployment policies.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    CachePolicyConfig, CachePolicyCookieBehavior, CachePolicyCookiesConfig,
    CachePolicyHeaderBehavior, CachePolicyHeadersConfig, CachePolicyQueryStringBehavior,
    CachePolicyQueryStringsConfig, ContinuousDeploymentPolicyConfig,
    ContinuousDeploymentPolicyType, ContinuousDeploymentSingleHeaderConfig,
    OriginAccessControlConfig, OriginAccessControlOriginTypes, OriginAccessControlSigningBehaviors,
    OriginAccessControlSigningProtocols, OriginRequestPolicyConfig,
    OriginRequestPolicyCookieBehavior, OriginRequestPolicyCookiesConfig,
    OriginRequestPolicyHeaderBehavior, OriginRequestPolicyHeadersConfig,
    OriginRequestPolicyQueryStringBehavior, OriginRequestPolicyQueryStringsConfig,
    ParametersInCacheKeyAndForwardedToOrigin, ResponseHeadersPolicyAccessControlAllowHeaders,
    ResponseHeadersPolicyAccessControlAllowMethods,
    ResponseHeadersPolicyAccessControlAllowMethodsValues,
    ResponseHeadersPolicyAccessControlAllowOrigins, ResponseHeadersPolicyConfig,
    ResponseHeadersPolicyCorsConfig, StagingDistributionDnsNames, TrafficConfig,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

fn oac_cfg(name: &str) -> OriginAccessControlConfig {
    OriginAccessControlConfig::builder()
        .name(name)
        .origin_access_control_origin_type(OriginAccessControlOriginTypes::S3)
        .signing_behavior(OriginAccessControlSigningBehaviors::Always)
        .signing_protocol(OriginAccessControlSigningProtocols::Sigv4)
        .build()
        .unwrap()
}

fn cache_cfg(name: &str) -> CachePolicyConfig {
    CachePolicyConfig::builder()
        .name(name)
        .min_ttl(0)
        .parameters_in_cache_key_and_forwarded_to_origin(
            ParametersInCacheKeyAndForwardedToOrigin::builder()
                .enable_accept_encoding_gzip(false)
                .headers_config(
                    CachePolicyHeadersConfig::builder()
                        .header_behavior(CachePolicyHeaderBehavior::None)
                        .build()
                        .unwrap(),
                )
                .cookies_config(
                    CachePolicyCookiesConfig::builder()
                        .cookie_behavior(CachePolicyCookieBehavior::None)
                        .build()
                        .unwrap(),
                )
                .query_strings_config(
                    CachePolicyQueryStringsConfig::builder()
                        .query_string_behavior(CachePolicyQueryStringBehavior::None)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn orp_cfg(name: &str) -> OriginRequestPolicyConfig {
    OriginRequestPolicyConfig::builder()
        .name(name)
        .headers_config(
            OriginRequestPolicyHeadersConfig::builder()
                .header_behavior(OriginRequestPolicyHeaderBehavior::None)
                .build()
                .unwrap(),
        )
        .cookies_config(
            OriginRequestPolicyCookiesConfig::builder()
                .cookie_behavior(OriginRequestPolicyCookieBehavior::None)
                .build()
                .unwrap(),
        )
        .query_strings_config(
            OriginRequestPolicyQueryStringsConfig::builder()
                .query_string_behavior(OriginRequestPolicyQueryStringBehavior::None)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn rhp_cfg(name: &str) -> ResponseHeadersPolicyConfig {
    ResponseHeadersPolicyConfig::builder()
        .name(name)
        .cors_config(
            ResponseHeadersPolicyCorsConfig::builder()
                .access_control_allow_credentials(false)
                .access_control_allow_headers(
                    ResponseHeadersPolicyAccessControlAllowHeaders::builder()
                        .quantity(1)
                        .items("*")
                        .build()
                        .unwrap(),
                )
                .access_control_allow_methods(
                    ResponseHeadersPolicyAccessControlAllowMethods::builder()
                        .quantity(1)
                        .items(ResponseHeadersPolicyAccessControlAllowMethodsValues::Get)
                        .build()
                        .unwrap(),
                )
                .access_control_allow_origins(
                    ResponseHeadersPolicyAccessControlAllowOrigins::builder()
                        .quantity(1)
                        .items("*")
                        .build()
                        .unwrap(),
                )
                .origin_override(true)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn cdp_cfg() -> ContinuousDeploymentPolicyConfig {
    ContinuousDeploymentPolicyConfig::builder()
        .staging_distribution_dns_names(
            StagingDistributionDnsNames::builder()
                .quantity(1)
                .items("staging.example.com")
                .build()
                .unwrap(),
        )
        .enabled(true)
        .traffic_config(
            TrafficConfig::builder()
                .r#type(ContinuousDeploymentPolicyType::SingleHeader)
                .single_header_config(
                    ContinuousDeploymentSingleHeaderConfig::builder()
                        .header("aws-cf-cd-staging")
                        .value("true")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

// ─── Origin Access Control ────────────────────────────────────────────

#[test_action("cloudfront", "CreateOriginAccessControl", checksum = "b0d2363e")]
#[tokio::test]
async fn cloudfront_create_oac() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_origin_access_control()
        .origin_access_control_config(oac_cfg("conf-oac-1"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetOriginAccessControl", checksum = "f20a62bc")]
#[tokio::test]
async fn cloudfront_get_oac() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_access_control()
        .origin_access_control_config(oac_cfg("conf-oac-2"))
        .send()
        .await
        .unwrap();
    let id = create.origin_access_control().unwrap().id().to_string();
    cf.get_origin_access_control().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "GetOriginAccessControlConfig", checksum = "39cb83c0")]
#[tokio::test]
async fn cloudfront_get_oac_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_access_control()
        .origin_access_control_config(oac_cfg("conf-oac-3"))
        .send()
        .await
        .unwrap();
    let id = create.origin_access_control().unwrap().id().to_string();
    cf.get_origin_access_control_config()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateOriginAccessControl", checksum = "cbf68e2f")]
#[tokio::test]
async fn cloudfront_update_oac() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_access_control()
        .origin_access_control_config(oac_cfg("conf-oac-4"))
        .send()
        .await
        .unwrap();
    let id = create.origin_access_control().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.update_origin_access_control()
        .id(&id)
        .if_match(&etag)
        .origin_access_control_config(oac_cfg("conf-oac-4-updated"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteOriginAccessControl", checksum = "ec5f8552")]
#[tokio::test]
async fn cloudfront_delete_oac() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_access_control()
        .origin_access_control_config(oac_cfg("conf-oac-5"))
        .send()
        .await
        .unwrap();
    let id = create.origin_access_control().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.delete_origin_access_control()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListOriginAccessControls", checksum = "05e3e15f")]
#[tokio::test]
async fn cloudfront_list_oac() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_origin_access_controls().send().await.unwrap();
}

// ─── Cache Policy ─────────────────────────────────────────────────────

#[test_action("cloudfront", "CreateCachePolicy", checksum = "f5cf05a9")]
#[tokio::test]
async fn cloudfront_create_cache_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_cache_policy()
        .cache_policy_config(cache_cfg("conf-cp-1"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetCachePolicy", checksum = "bb136706")]
#[tokio::test]
async fn cloudfront_get_cache_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_cache_policy()
        .cache_policy_config(cache_cfg("conf-cp-2"))
        .send()
        .await
        .unwrap();
    let id = create.cache_policy().unwrap().id().to_string();
    cf.get_cache_policy().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "GetCachePolicyConfig", checksum = "9c2b70f4")]
#[tokio::test]
async fn cloudfront_get_cache_policy_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_cache_policy()
        .cache_policy_config(cache_cfg("conf-cp-3"))
        .send()
        .await
        .unwrap();
    let id = create.cache_policy().unwrap().id().to_string();
    cf.get_cache_policy_config().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "UpdateCachePolicy", checksum = "6e7e0f85")]
#[tokio::test]
async fn cloudfront_update_cache_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_cache_policy()
        .cache_policy_config(cache_cfg("conf-cp-4"))
        .send()
        .await
        .unwrap();
    let id = create.cache_policy().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.update_cache_policy()
        .id(&id)
        .if_match(&etag)
        .cache_policy_config(cache_cfg("conf-cp-4-updated"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteCachePolicy", checksum = "54d0fda4")]
#[tokio::test]
async fn cloudfront_delete_cache_policy() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_cache_policy()
        .cache_policy_config(cache_cfg("conf-cp-5"))
        .send()
        .await
        .unwrap();
    let id = create.cache_policy().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.delete_cache_policy()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListCachePolicies", checksum = "792038f5")]
#[tokio::test]
async fn cloudfront_list_cache_policies() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_cache_policies().send().await.unwrap();
}

// ─── Origin Request Policy ────────────────────────────────────────────

#[test_action("cloudfront", "CreateOriginRequestPolicy", checksum = "7ce8ece6")]
#[tokio::test]
async fn cloudfront_create_orp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_origin_request_policy()
        .origin_request_policy_config(orp_cfg("conf-orp-1"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetOriginRequestPolicy", checksum = "3e460804")]
#[tokio::test]
async fn cloudfront_get_orp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_request_policy()
        .origin_request_policy_config(orp_cfg("conf-orp-2"))
        .send()
        .await
        .unwrap();
    let id = create.origin_request_policy().unwrap().id().to_string();
    cf.get_origin_request_policy().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "GetOriginRequestPolicyConfig", checksum = "66a5d04d")]
#[tokio::test]
async fn cloudfront_get_orp_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_request_policy()
        .origin_request_policy_config(orp_cfg("conf-orp-3"))
        .send()
        .await
        .unwrap();
    let id = create.origin_request_policy().unwrap().id().to_string();
    cf.get_origin_request_policy_config()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateOriginRequestPolicy", checksum = "ec4ca50e")]
#[tokio::test]
async fn cloudfront_update_orp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_request_policy()
        .origin_request_policy_config(orp_cfg("conf-orp-4"))
        .send()
        .await
        .unwrap();
    let id = create.origin_request_policy().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.update_origin_request_policy()
        .id(&id)
        .if_match(&etag)
        .origin_request_policy_config(orp_cfg("conf-orp-4-updated"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteOriginRequestPolicy", checksum = "8c0709fe")]
#[tokio::test]
async fn cloudfront_delete_orp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_origin_request_policy()
        .origin_request_policy_config(orp_cfg("conf-orp-5"))
        .send()
        .await
        .unwrap();
    let id = create.origin_request_policy().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.delete_origin_request_policy()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListOriginRequestPolicies", checksum = "0b630441")]
#[tokio::test]
async fn cloudfront_list_orp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_origin_request_policies().send().await.unwrap();
}

// ─── Response Headers Policy ──────────────────────────────────────────

#[test_action("cloudfront", "CreateResponseHeadersPolicy", checksum = "56ba5ee4")]
#[tokio::test]
async fn cloudfront_create_rhp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_response_headers_policy()
        .response_headers_policy_config(rhp_cfg("conf-rhp-1"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetResponseHeadersPolicy", checksum = "50650ae2")]
#[tokio::test]
async fn cloudfront_get_rhp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_response_headers_policy()
        .response_headers_policy_config(rhp_cfg("conf-rhp-2"))
        .send()
        .await
        .unwrap();
    let id = create.response_headers_policy().unwrap().id().to_string();
    cf.get_response_headers_policy()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetResponseHeadersPolicyConfig", checksum = "c5f37364")]
#[tokio::test]
async fn cloudfront_get_rhp_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_response_headers_policy()
        .response_headers_policy_config(rhp_cfg("conf-rhp-3"))
        .send()
        .await
        .unwrap();
    let id = create.response_headers_policy().unwrap().id().to_string();
    cf.get_response_headers_policy_config()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateResponseHeadersPolicy", checksum = "16926921")]
#[tokio::test]
async fn cloudfront_update_rhp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_response_headers_policy()
        .response_headers_policy_config(rhp_cfg("conf-rhp-4"))
        .send()
        .await
        .unwrap();
    let id = create.response_headers_policy().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.update_response_headers_policy()
        .id(&id)
        .if_match(&etag)
        .response_headers_policy_config(rhp_cfg("conf-rhp-4-updated"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteResponseHeadersPolicy", checksum = "2e942a96")]
#[tokio::test]
async fn cloudfront_delete_rhp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_response_headers_policy()
        .response_headers_policy_config(rhp_cfg("conf-rhp-5"))
        .send()
        .await
        .unwrap();
    let id = create.response_headers_policy().unwrap().id().to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.delete_response_headers_policy()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListResponseHeadersPolicies", checksum = "107dfed9")]
#[tokio::test]
async fn cloudfront_list_rhp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_response_headers_policies().send().await.unwrap();
}

// ─── Continuous Deployment Policy ─────────────────────────────────────

#[test_action(
    "cloudfront",
    "CreateContinuousDeploymentPolicy",
    checksum = "1d68722d"
)]
#[tokio::test]
async fn cloudfront_create_cdp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_continuous_deployment_policy()
        .continuous_deployment_policy_config(cdp_cfg())
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetContinuousDeploymentPolicy", checksum = "9a7b09f7")]
#[tokio::test]
async fn cloudfront_get_cdp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_continuous_deployment_policy()
        .continuous_deployment_policy_config(cdp_cfg())
        .send()
        .await
        .unwrap();
    let id = create
        .continuous_deployment_policy()
        .unwrap()
        .id()
        .to_string();
    cf.get_continuous_deployment_policy()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "GetContinuousDeploymentPolicyConfig",
    checksum = "c8d8580a"
)]
#[tokio::test]
async fn cloudfront_get_cdp_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_continuous_deployment_policy()
        .continuous_deployment_policy_config(cdp_cfg())
        .send()
        .await
        .unwrap();
    let id = create
        .continuous_deployment_policy()
        .unwrap()
        .id()
        .to_string();
    cf.get_continuous_deployment_policy_config()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "UpdateContinuousDeploymentPolicy",
    checksum = "c9b379a1"
)]
#[tokio::test]
async fn cloudfront_update_cdp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_continuous_deployment_policy()
        .continuous_deployment_policy_config(cdp_cfg())
        .send()
        .await
        .unwrap();
    let id = create
        .continuous_deployment_policy()
        .unwrap()
        .id()
        .to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.update_continuous_deployment_policy()
        .id(&id)
        .if_match(&etag)
        .continuous_deployment_policy_config(cdp_cfg())
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "DeleteContinuousDeploymentPolicy",
    checksum = "c3294626"
)]
#[tokio::test]
async fn cloudfront_delete_cdp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_continuous_deployment_policy()
        .continuous_deployment_policy_config(cdp_cfg())
        .send()
        .await
        .unwrap();
    let id = create
        .continuous_deployment_policy()
        .unwrap()
        .id()
        .to_string();
    let etag = create.e_tag().unwrap().to_string();
    cf.delete_continuous_deployment_policy()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListContinuousDeploymentPolicies",
    checksum = "e7bbc957"
)]
#[tokio::test]
async fn cloudfront_list_cdp() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_continuous_deployment_policies()
        .send()
        .await
        .unwrap();
}
