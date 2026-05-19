//! CloudFront Batch 3 conformance tests: Functions, Public Keys,
//! Key Groups, Key Value Stores, Origin Access Identities (legacy),
//! Monitoring Subscriptions.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::primitives::Blob;
use aws_sdk_cloudfront::types::{
    CloudFrontOriginAccessIdentityConfig, DefaultCacheBehavior, DistributionConfig, FunctionConfig,
    FunctionRuntime, FunctionStage, KeyGroupConfig, MonitoringSubscription, Origin, Origins,
    PublicKeyConfig, RealtimeMetricsSubscriptionConfig, RealtimeMetricsSubscriptionStatus,
    ViewerProtocolPolicy,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const SAMPLE_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALfm1u9C7VXhhRnD\n-----END PUBLIC KEY-----";

fn fn_cfg() -> FunctionConfig {
    FunctionConfig::builder()
        .comment("conf fn")
        .runtime(FunctionRuntime::CloudfrontJs20)
        .build()
        .unwrap()
}

fn pk_cfg(caller_ref: &str, name: &str) -> PublicKeyConfig {
    PublicKeyConfig::builder()
        .caller_reference(caller_ref)
        .name(name)
        .encoded_key(SAMPLE_KEY)
        .build()
        .unwrap()
}

fn kg_cfg(name: &str, public_key: &str) -> KeyGroupConfig {
    KeyGroupConfig::builder()
        .name(name)
        .items(public_key)
        .build()
        .unwrap()
}

fn oai_cfg(caller_ref: &str) -> CloudFrontOriginAccessIdentityConfig {
    CloudFrontOriginAccessIdentityConfig::builder()
        .caller_reference(caller_ref)
        .comment("conf oai")
        .build()
        .unwrap()
}

fn dist_cfg(caller_ref: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("monsub")
        .enabled(true)
        .origins(
            Origins::builder()
                .quantity(1)
                .items(
                    Origin::builder()
                        .id("o")
                        .domain_name("example.com")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id("o")
                .viewer_protocol_policy(ViewerProtocolPolicy::AllowAll)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

// ─── Functions ────────────────────────────────────────────────────────

#[test_action("cloudfront", "CreateFunction", checksum = "f9ca76f8")]
#[tokio::test]
async fn cf_create_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_function()
        .name("conf-fn-1")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DescribeFunction", checksum = "517683e8")]
#[tokio::test]
async fn cf_describe_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_function()
        .name("conf-fn-2")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    cf.describe_function()
        .name("conf-fn-2")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetFunction", checksum = "13f22787")]
#[tokio::test]
async fn cf_get_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_function()
        .name("conf-fn-3")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    cf.get_function()
        .name("conf-fn-3")
        .stage(FunctionStage::Development)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateFunction", checksum = "ba4cc899")]
#[tokio::test]
async fn cf_update_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_function()
        .name("conf-fn-4")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    let etag = create.e_tag().unwrap().to_string();
    cf.update_function()
        .name("conf-fn-4")
        .if_match(&etag)
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.response;}"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteFunction", checksum = "d2797b3c")]
#[tokio::test]
async fn cf_delete_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_function()
        .name("conf-fn-5")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    let etag = create.e_tag().unwrap().to_string();
    cf.delete_function()
        .name("conf-fn-5")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListFunctions", checksum = "95432df7")]
#[tokio::test]
async fn cf_list_functions() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_functions().send().await.unwrap();
}

#[test_action("cloudfront", "PublishFunction", checksum = "7ea1cf56")]
#[tokio::test]
async fn cf_publish_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_function()
        .name("conf-fn-6")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    let etag = create.e_tag().unwrap().to_string();
    cf.publish_function()
        .name("conf-fn-6")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "TestFunction", checksum = "b72974d2")]
#[tokio::test]
async fn cf_test_function() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let create = cf
        .create_function()
        .name("conf-fn-7")
        .function_config(fn_cfg())
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    let etag = create.e_tag().unwrap().to_string();
    cf.test_function()
        .name("conf-fn-7")
        .if_match(&etag)
        .stage(FunctionStage::Development)
        .event_object(Blob::new(b"{}"))
        .send()
        .await
        .unwrap();
}

// ─── Public Keys ──────────────────────────────────────────────────────

#[test_action("cloudfront", "CreatePublicKey", checksum = "9bd50a30")]
#[tokio::test]
async fn cf_create_public_key() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_public_key()
        .public_key_config(pk_cfg("conf-pk-1", "conf-pk-1"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetPublicKey", checksum = "3dde09ff")]
#[tokio::test]
async fn cf_get_public_key() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_public_key()
        .public_key_config(pk_cfg("conf-pk-2", "conf-pk-2"))
        .send()
        .await
        .unwrap();
    let id = c.public_key().unwrap().id().to_string();
    cf.get_public_key().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "GetPublicKeyConfig", checksum = "ae85f0c1")]
#[tokio::test]
async fn cf_get_public_key_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_public_key()
        .public_key_config(pk_cfg("conf-pk-3", "conf-pk-3"))
        .send()
        .await
        .unwrap();
    let id = c.public_key().unwrap().id().to_string();
    cf.get_public_key_config().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "UpdatePublicKey", checksum = "24c300c5")]
#[tokio::test]
async fn cf_update_public_key() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_public_key()
        .public_key_config(pk_cfg("conf-pk-4", "conf-pk-4"))
        .send()
        .await
        .unwrap();
    let id = c.public_key().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_public_key()
        .id(&id)
        .if_match(&etag)
        .public_key_config(pk_cfg("conf-pk-4", "conf-pk-4-renamed"))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeletePublicKey", checksum = "153921f2")]
#[tokio::test]
async fn cf_delete_public_key() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_public_key()
        .public_key_config(pk_cfg("conf-pk-5", "conf-pk-5"))
        .send()
        .await
        .unwrap();
    let id = c.public_key().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_public_key()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListPublicKeys", checksum = "975977ca")]
#[tokio::test]
async fn cf_list_public_keys() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_public_keys().send().await.unwrap();
}

// ─── Key Groups ───────────────────────────────────────────────────────

async fn make_pk(server: &TestServer, caller_ref: &str) -> String {
    let cf = server.cloudfront_client().await;
    cf.create_public_key()
        .public_key_config(pk_cfg(caller_ref, caller_ref))
        .send()
        .await
        .unwrap()
        .public_key()
        .unwrap()
        .id()
        .to_string()
}

#[test_action("cloudfront", "CreateKeyGroup", checksum = "c9e842df")]
#[tokio::test]
async fn cf_create_key_group() {
    let server = TestServer::start().await;
    let pk = make_pk(&server, "conf-kg-pk-1").await;
    let cf = server.cloudfront_client().await;
    cf.create_key_group()
        .key_group_config(kg_cfg("conf-kg-1", &pk))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetKeyGroup", checksum = "2244de9f")]
#[tokio::test]
async fn cf_get_key_group() {
    let server = TestServer::start().await;
    let pk = make_pk(&server, "conf-kg-pk-2").await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_key_group()
        .key_group_config(kg_cfg("conf-kg-2", &pk))
        .send()
        .await
        .unwrap();
    let id = c.key_group().unwrap().id().to_string();
    cf.get_key_group().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "GetKeyGroupConfig", checksum = "c770f625")]
#[tokio::test]
async fn cf_get_key_group_config() {
    let server = TestServer::start().await;
    let pk = make_pk(&server, "conf-kg-pk-3").await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_key_group()
        .key_group_config(kg_cfg("conf-kg-3", &pk))
        .send()
        .await
        .unwrap();
    let id = c.key_group().unwrap().id().to_string();
    cf.get_key_group_config().id(&id).send().await.unwrap();
}

#[test_action("cloudfront", "UpdateKeyGroup", checksum = "adf3010b")]
#[tokio::test]
async fn cf_update_key_group() {
    let server = TestServer::start().await;
    let pk = make_pk(&server, "conf-kg-pk-4").await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_key_group()
        .key_group_config(kg_cfg("conf-kg-4", &pk))
        .send()
        .await
        .unwrap();
    let id = c.key_group().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_key_group()
        .id(&id)
        .if_match(&etag)
        .key_group_config(kg_cfg("conf-kg-4-renamed", &pk))
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteKeyGroup", checksum = "b8fde1e9")]
#[tokio::test]
async fn cf_delete_key_group() {
    let server = TestServer::start().await;
    let pk = make_pk(&server, "conf-kg-pk-5").await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_key_group()
        .key_group_config(kg_cfg("conf-kg-5", &pk))
        .send()
        .await
        .unwrap();
    let id = c.key_group().unwrap().id().to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_key_group()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListKeyGroups", checksum = "ab654cc9")]
#[tokio::test]
async fn cf_list_key_groups() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_key_groups().send().await.unwrap();
}

// ─── Key Value Stores ─────────────────────────────────────────────────

#[test_action("cloudfront", "CreateKeyValueStore", checksum = "09d950c6")]
#[tokio::test]
async fn cf_create_key_value_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_key_value_store()
        .name("conf-kvs-1")
        .comment("c")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DescribeKeyValueStore", checksum = "4d05f4ed")]
#[tokio::test]
async fn cf_describe_key_value_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_key_value_store()
        .name("conf-kvs-2")
        .send()
        .await
        .unwrap();
    cf.describe_key_value_store()
        .name("conf-kvs-2")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "UpdateKeyValueStore", checksum = "5ea06309")]
#[tokio::test]
async fn cf_update_key_value_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_key_value_store()
        .name("conf-kvs-3")
        .send()
        .await
        .unwrap();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_key_value_store()
        .name("conf-kvs-3")
        .if_match(&etag)
        .comment("updated")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteKeyValueStore", checksum = "7f405669")]
#[tokio::test]
async fn cf_delete_key_value_store() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_key_value_store()
        .name("conf-kvs-4")
        .send()
        .await
        .unwrap();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_key_value_store()
        .name("conf-kvs-4")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "ListKeyValueStores", checksum = "2cad57f2")]
#[tokio::test]
async fn cf_list_key_value_stores() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_key_value_stores().send().await.unwrap();
}

// ─── Origin Access Identities ─────────────────────────────────────────

#[test_action(
    "cloudfront",
    "CreateCloudFrontOriginAccessIdentity",
    checksum = "f724fc9f"
)]
#[tokio::test]
async fn cf_create_oai() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(oai_cfg("conf-oai-1"))
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "GetCloudFrontOriginAccessIdentity",
    checksum = "bebdcbe5"
)]
#[tokio::test]
async fn cf_get_oai() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(oai_cfg("conf-oai-2"))
        .send()
        .await
        .unwrap();
    let id = c
        .cloud_front_origin_access_identity()
        .unwrap()
        .id()
        .to_string();
    cf.get_cloud_front_origin_access_identity()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "GetCloudFrontOriginAccessIdentityConfig",
    checksum = "5fc5620c"
)]
#[tokio::test]
async fn cf_get_oai_config() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(oai_cfg("conf-oai-3"))
        .send()
        .await
        .unwrap();
    let id = c
        .cloud_front_origin_access_identity()
        .unwrap()
        .id()
        .to_string();
    cf.get_cloud_front_origin_access_identity_config()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "UpdateCloudFrontOriginAccessIdentity",
    checksum = "1774658b"
)]
#[tokio::test]
async fn cf_update_oai() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(oai_cfg("conf-oai-4"))
        .send()
        .await
        .unwrap();
    let id = c
        .cloud_front_origin_access_identity()
        .unwrap()
        .id()
        .to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.update_cloud_front_origin_access_identity()
        .id(&id)
        .if_match(&etag)
        .cloud_front_origin_access_identity_config(oai_cfg("conf-oai-4"))
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "DeleteCloudFrontOriginAccessIdentity",
    checksum = "03747aec"
)]
#[tokio::test]
async fn cf_delete_oai() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    let c = cf
        .create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(oai_cfg("conf-oai-5"))
        .send()
        .await
        .unwrap();
    let id = c
        .cloud_front_origin_access_identity()
        .unwrap()
        .id()
        .to_string();
    let etag = c.e_tag().unwrap().to_string();
    cf.delete_cloud_front_origin_access_identity()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "cloudfront",
    "ListCloudFrontOriginAccessIdentities",
    checksum = "7e775097"
)]
#[tokio::test]
async fn cf_list_oai() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.list_cloud_front_origin_access_identities()
        .send()
        .await
        .unwrap();
}

// ─── Monitoring Subscriptions ─────────────────────────────────────────

async fn make_dist(server: &TestServer, caller_ref: &str) -> String {
    let cf = server.cloudfront_client().await;
    cf.create_distribution()
        .distribution_config(dist_cfg(caller_ref))
        .send()
        .await
        .unwrap()
        .distribution()
        .unwrap()
        .id()
        .to_string()
}

#[test_action("cloudfront", "CreateMonitoringSubscription", checksum = "2861cb5a")]
#[tokio::test]
async fn cf_create_monsub() {
    let server = TestServer::start().await;
    let id = make_dist(&server, "conf-monsub-1").await;
    let cf = server.cloudfront_client().await;
    cf.create_monitoring_subscription()
        .distribution_id(&id)
        .monitoring_subscription(
            MonitoringSubscription::builder()
                .realtime_metrics_subscription_config(
                    RealtimeMetricsSubscriptionConfig::builder()
                        .realtime_metrics_subscription_status(
                            RealtimeMetricsSubscriptionStatus::Enabled,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "GetMonitoringSubscription", checksum = "67221543")]
#[tokio::test]
async fn cf_get_monsub() {
    let server = TestServer::start().await;
    let id = make_dist(&server, "conf-monsub-2").await;
    let cf = server.cloudfront_client().await;
    cf.create_monitoring_subscription()
        .distribution_id(&id)
        .monitoring_subscription(
            MonitoringSubscription::builder()
                .realtime_metrics_subscription_config(
                    RealtimeMetricsSubscriptionConfig::builder()
                        .realtime_metrics_subscription_status(
                            RealtimeMetricsSubscriptionStatus::Enabled,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    cf.get_monitoring_subscription()
        .distribution_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("cloudfront", "DeleteMonitoringSubscription", checksum = "c45f9fc3")]
#[tokio::test]
async fn cf_delete_monsub() {
    let server = TestServer::start().await;
    let id = make_dist(&server, "conf-monsub-3").await;
    let cf = server.cloudfront_client().await;
    cf.create_monitoring_subscription()
        .distribution_id(&id)
        .monitoring_subscription(
            MonitoringSubscription::builder()
                .realtime_metrics_subscription_config(
                    RealtimeMetricsSubscriptionConfig::builder()
                        .realtime_metrics_subscription_status(
                            RealtimeMetricsSubscriptionStatus::Enabled,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    cf.delete_monitoring_subscription()
        .distribution_id(&id)
        .send()
        .await
        .unwrap();
}
