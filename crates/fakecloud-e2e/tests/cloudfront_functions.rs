//! CloudFront Batch 3 E2E: Functions, Public Keys, Key Groups,
//! Key Value Stores, Origin Access Identities (legacy), Monitoring
//! Subscriptions.
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::primitives::Blob;
use aws_sdk_cloudfront::types::{
    CloudFrontOriginAccessIdentityConfig, FunctionConfig, FunctionRuntime, ImportSource,
    ImportSourceType, KeyGroupConfig, MonitoringSubscription, PublicKeyConfig,
    RealtimeMetricsSubscriptionConfig, RealtimeMetricsSubscriptionStatus,
};
use helpers::TestServer;

const SAMPLE_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALfm1u9C7VXhhRnD\n-----END PUBLIC KEY-----";

#[tokio::test]
async fn function_lifecycle() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let cfg = FunctionConfig::builder()
        .comment("e2e fn")
        .runtime(FunctionRuntime::CloudfrontJs20)
        .build()
        .unwrap();

    let create = cf
        .create_function()
        .name("e2e-fn")
        .function_config(cfg.clone())
        .function_code(Blob::new(
            b"function handler(event) { return event.request; }",
        ))
        .send()
        .await
        .expect("create");
    let etag = create.e_tag().unwrap().to_string();

    let _ = cf
        .describe_function()
        .name("e2e-fn")
        .send()
        .await
        .expect("describe");

    let updated = cf
        .update_function()
        .name("e2e-fn")
        .if_match(&etag)
        .function_config(cfg)
        .function_code(Blob::new(
            b"function handler(event) { return event.response; }",
        ))
        .send()
        .await
        .expect("update");
    let new_etag = updated.e_tag().unwrap().to_string();

    cf.publish_function()
        .name("e2e-fn")
        .if_match(&new_etag)
        .send()
        .await
        .expect("publish");

    // After publish, fetch latest etag via describe.
    let described = cf
        .describe_function()
        .name("e2e-fn")
        .send()
        .await
        .expect("describe-after-publish");
    let pub_etag = described.e_tag().unwrap().to_string();

    cf.delete_function()
        .name("e2e-fn")
        .if_match(&pub_etag)
        .send()
        .await
        .expect("delete");
}

#[tokio::test]
async fn public_key_and_key_group_lifecycle() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let pk_create = cf
        .create_public_key()
        .public_key_config(
            PublicKeyConfig::builder()
                .caller_reference("pk-1")
                .name("pk-1")
                .encoded_key(SAMPLE_KEY)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_public_key");
    let pk_id = pk_create.public_key().unwrap().id().to_string();
    let pk_etag = pk_create.e_tag().unwrap().to_string();

    let kg_create = cf
        .create_key_group()
        .key_group_config(
            KeyGroupConfig::builder()
                .name("kg-1")
                .items(pk_id.clone())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_key_group");
    let kg_id = kg_create.key_group().unwrap().id().to_string();
    let kg_etag = kg_create.e_tag().unwrap().to_string();

    cf.delete_key_group()
        .id(&kg_id)
        .if_match(&kg_etag)
        .send()
        .await
        .expect("delete_kg");

    cf.delete_public_key()
        .id(&pk_id)
        .if_match(&pk_etag)
        .send()
        .await
        .expect("delete_pk");
}

#[tokio::test]
async fn key_value_store_lifecycle() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let create = cf
        .create_key_value_store()
        .name("kvs-1")
        .comment("test")
        .import_source(
            ImportSource::builder()
                .source_type(ImportSourceType::S3)
                .source_arn("arn:aws:s3:::bucket/seed.json")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_kvs");
    let etag = create.e_tag().unwrap().to_string();

    let described = cf
        .describe_key_value_store()
        .name("kvs-1")
        .send()
        .await
        .expect("describe_kvs");
    assert_eq!(described.key_value_store().unwrap().name(), "kvs-1");

    cf.delete_key_value_store()
        .name("kvs-1")
        .if_match(&etag)
        .send()
        .await
        .expect("delete_kvs");
}

#[tokio::test]
async fn origin_access_identity_lifecycle() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let create = cf
        .create_cloud_front_origin_access_identity()
        .cloud_front_origin_access_identity_config(
            CloudFrontOriginAccessIdentityConfig::builder()
                .caller_reference("oai-1")
                .comment("e2e oai")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_oai");
    let id = create
        .cloud_front_origin_access_identity()
        .unwrap()
        .id()
        .to_string();
    let etag = create.e_tag().unwrap().to_string();

    cf.delete_cloud_front_origin_access_identity()
        .id(&id)
        .if_match(&etag)
        .send()
        .await
        .expect("delete_oai");
}

#[tokio::test]
async fn monitoring_subscription_lifecycle() {
    use aws_sdk_cloudfront::types::{
        DefaultCacheBehavior, DistributionConfig, Origin, Origins, ViewerProtocolPolicy,
    };

    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let dist = cf
        .create_distribution()
        .distribution_config(
            DistributionConfig::builder()
                .caller_reference("monsub-1")
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
                        .cache_policy_id("658327ea-f89d-4fab-a63d-7e88639e58f6")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_dist");
    let dist_id = dist.distribution().unwrap().id().to_string();

    cf.create_monitoring_subscription()
        .distribution_id(&dist_id)
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
        .expect("create_monsub");

    cf.get_monitoring_subscription()
        .distribution_id(&dist_id)
        .send()
        .await
        .expect("get_monsub");

    cf.delete_monitoring_subscription()
        .distribution_id(&dist_id)
        .send()
        .await
        .expect("delete_monsub");
}

#[tokio::test]
async fn test_function_rejects_stale_etag() {
    use aws_sdk_cloudfront::types::FunctionStage;
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;
    cf.create_function()
        .name("e2e-fn-stale")
        .function_config(
            FunctionConfig::builder()
                .comment("stale-test")
                .runtime(FunctionRuntime::CloudfrontJs20)
                .build()
                .unwrap(),
        )
        .function_code(Blob::new(b"function handler(e){return e.request;}"))
        .send()
        .await
        .unwrap();
    let res = cf
        .test_function()
        .name("e2e-fn-stale")
        .if_match("E_NOT_MATCHING")
        .stage(FunctionStage::Development)
        .event_object(Blob::new(b"{}"))
        .send()
        .await;
    assert!(res.is_err(), "stale If-Match must be rejected");
}
