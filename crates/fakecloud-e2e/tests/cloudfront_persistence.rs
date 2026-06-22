//! CloudFront persistence (restart-recovery) E2E tests.
#![allow(deprecated)]

mod helpers;

use std::path::Path;

use aws_sdk_cloudfront::types::{
    CookiePreference, DefaultCacheBehavior, DistributionConfig, ForwardedValues, Headers,
    ItemSelection, Origin, Origins, PublicKeyConfig, ViewerProtocolPolicy,
};
use helpers::TestServer;

const SAMPLE_KEY: &str =
    "-----BEGIN PUBLIC KEY-----\nMFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALfm1u9C7VXhhRnD\n-----END PUBLIC KEY-----";

fn minimal_config(caller_ref: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("persist")
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

async fn start(data_path: &Path, status_delay: &str) -> TestServer {
    TestServer::start_full(
        &[
            ("FAKECLOUD_CONTAINER_CLI", "false"),
            ("FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC", status_delay),
        ],
        &[
            "--storage-mode",
            "persistent",
            "--data-path",
            &data_path.display().to_string(),
        ],
    )
    .await
}

async fn wait_for_status(cf: &aws_sdk_cloudfront::Client, id: &str, want: &str) {
    for _ in 0..50 {
        let st = cf
            .get_distribution()
            .id(id)
            .send()
            .await
            .unwrap()
            .distribution()
            .map(|d| d.status().to_string());
        if st.as_deref() == Some(want) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("distribution never reached {want}");
}

/// A deployed distribution and a public key survive a restart. The public key
/// is created AFTER the distribution reaches Deployed, forcing a durable
/// snapshot that captures the propagation transition.
#[tokio::test]
async fn persistence_distribution_and_public_key_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = start(tmp.path(), "0").await;
    let cf = server.cloudfront_client().await;

    let dist_id = cf
        .create_distribution()
        .distribution_config(minimal_config("persist-dist-1"))
        .send()
        .await
        .unwrap()
        .distribution()
        .unwrap()
        .id()
        .to_string();

    wait_for_status(&cf, &dist_id, "Deployed").await;

    cf.create_public_key()
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
        .unwrap();

    server.restart().await;
    let cf = server.cloudfront_client().await;

    let dist = cf.get_distribution().id(&dist_id).send().await.unwrap();
    assert_eq!(dist.distribution().unwrap().status(), "Deployed");

    let keys = cf.list_public_keys().send().await.unwrap();
    assert!(keys
        .public_key_list()
        .map(|l| l.items().iter().any(|k| k.name() == "pk-1"))
        .unwrap_or(false));
}

/// A distribution still InProgress at restart survives and the re-armed
/// propagation tick still drives it to Deployed afterward.
#[tokio::test]
async fn persistence_inprogress_distribution_reissues_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    // Long propagation delay so the distribution is still InProgress at restart.
    let mut server = start(tmp.path(), "3").await;
    let cf = server.cloudfront_client().await;

    let dist_id = cf
        .create_distribution()
        .distribution_config(minimal_config("persist-dist-inprog"))
        .send()
        .await
        .unwrap()
        .distribution()
        .unwrap()
        .id()
        .to_string();

    // Restart well before the 3s tick fires; persists as InProgress.
    server.restart().await;
    let cf = server.cloudfront_client().await;

    // Survives restart, then the re-armed tick drives it to Deployed.
    assert!(cf.get_distribution().id(&dist_id).send().await.is_ok());
    wait_for_status(&cf, &dist_id, "Deployed").await;
}
