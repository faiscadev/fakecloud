//! CloudFront `Status` lifecycle E2E tests.
//!
//! Real CloudFront returns `InProgress` immediately after Create/Update
//! and flips to `Deployed` once the edge propagation completes. fakecloud
//! mirrors that with a configurable delay defaulting to 1s; these tests
//! pin the delay long enough to observe the `InProgress` state and then
//! either wait for the auto-tick (short delay) or force the transition
//! via the `_fakecloud` admin endpoint (long delay).
#![allow(deprecated)]

mod helpers;

use aws_sdk_cloudfront::types::{
    CookiePreference, DefaultCacheBehavior, DistributionConfig, ForwardedValues, Headers,
    ItemSelection, Origin, Origins, ViewerProtocolPolicy,
};
use helpers::TestServer;

fn minimal_config(caller_ref: &str) -> DistributionConfig {
    DistributionConfig::builder()
        .caller_reference(caller_ref)
        .comment("e2e-status")
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

/// With a small auto-tick delay (0s), CreateDistribution still returns
/// `InProgress` synchronously in the response body — but a subsequent
/// `GetDistribution` after the tick has fired returns `Deployed`. This
/// exercises the lazy/spawned transition path.
#[tokio::test]
async fn create_distribution_status_transitions_to_deployed() {
    // 0s delay means the spawned task flips the status as soon as it gets
    // scheduled. Ours is the fastest delivery the runtime can manage, so a
    // short sleep below is enough for the deploy task to run.
    let server =
        TestServer::start_with_env(&[("FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC", "0")]).await;
    let cf = server.cloudfront_client().await;

    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("status-tick-ref"))
        .send()
        .await
        .expect("create_distribution");
    let dist = create.distribution().expect("distribution returned");
    let id = dist.id().to_string();
    assert_eq!(
        dist.status(),
        "InProgress",
        "CreateDistribution must return InProgress synchronously"
    );

    // Poll (bounded) for the spawned `schedule_distribution_deploy` task to
    // flip the status, rather than racing a single fixed sleep under CI load.
    assert!(
        poll_for_status(&cf, &id, "Deployed").await,
        "GetDistribution after the tick should report Deployed"
    );
}

/// Poll `GetDistribution` until the distribution reports `want` or a bounded
/// deadline elapses; returns whether the status was observed. Replaces a fixed
/// sleep so the assertion doesn't race the spawned deploy task under CI load.
async fn poll_for_status(cf: &aws_sdk_cloudfront::Client, id: &str, want: &str) -> bool {
    for _ in 0..50 {
        let st = cf
            .get_distribution()
            .id(id)
            .send()
            .await
            .expect("get_distribution")
            .distribution()
            .map(|d| d.status().to_string());
        if st.as_deref() == Some(want) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

/// With a long delay, the auto-tick can't race the assertion, so we use
/// the `_fakecloud` admin endpoint to force the flip. This proves both the
/// admin endpoint and the per-request env-var override work.
#[tokio::test]
async fn admin_endpoint_flips_status_synchronously() {
    let server =
        TestServer::start_with_env(&[("FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC", "60")]).await;
    let cf = server.cloudfront_client().await;

    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("status-admin-ref"))
        .send()
        .await
        .expect("create_distribution");
    let id = create.distribution().unwrap().id().to_string();

    // Status must still be InProgress because the 60s delay won't fire
    // during the test.
    let got = cf
        .get_distribution()
        .id(&id)
        .send()
        .await
        .expect("get_distribution");
    assert_eq!(got.distribution().unwrap().status(), "InProgress");

    // Force the flip via the admin endpoint.
    let url = format!(
        "{}/_fakecloud/cloudfront/distributions/{}/status",
        server.endpoint(),
        id
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({ "status": "Deployed" }))
        .send()
        .await
        .expect("admin status POST");
    assert_eq!(resp.status().as_u16(), 204);

    let got = cf
        .get_distribution()
        .id(&id)
        .send()
        .await
        .expect("get_distribution after flip");
    assert_eq!(got.distribution().unwrap().status(), "Deployed");
}

/// `GetDistribution` issued back-to-back with no intervening writes must
/// return the exact same ETag — clients depend on ETag stability for the
/// optimistic-concurrency If-Match flow.
#[tokio::test]
async fn etag_stable_across_repeated_reads() {
    let server = TestServer::start().await;
    let cf = server.cloudfront_client().await;

    let create = cf
        .create_distribution()
        .distribution_config(minimal_config("etag-stable-ref"))
        .send()
        .await
        .expect("create_distribution");
    let id = create.distribution().unwrap().id().to_string();
    let etag1 = create.e_tag().expect("etag header").to_string();

    let g1 = cf
        .get_distribution()
        .id(&id)
        .send()
        .await
        .expect("get_distribution 1");
    let g2 = cf
        .get_distribution()
        .id(&id)
        .send()
        .await
        .expect("get_distribution 2");
    assert_eq!(
        g1.e_tag(),
        g2.e_tag(),
        "GetDistribution ETag must be stable"
    );
    assert_eq!(g1.e_tag().unwrap(), etag1.as_str());
}

/// A no-op `UpdateDistribution` (PUT the same config back) must leave the
/// ETag unchanged. Real updates that mutate the config bump the ETag.
#[tokio::test]
async fn etag_only_bumps_on_real_changes() {
    // Long delay so the propagation tick can't race the assertion below.
    let server =
        TestServer::start_with_env(&[("FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC", "60")]).await;
    let cf = server.cloudfront_client().await;

    let initial = minimal_config("etag-bump-ref");
    let create = cf
        .create_distribution()
        .distribution_config(initial.clone())
        .send()
        .await
        .expect("create_distribution");
    let id = create.distribution().unwrap().id().to_string();
    let etag1 = create.e_tag().expect("etag").to_string();

    // Pull the current config + ETag back; this is what real callers use
    // as the basis for an UpdateDistribution.
    let cfg_resp = cf
        .get_distribution_config()
        .id(&id)
        .send()
        .await
        .expect("get_distribution_config");
    let if_match = cfg_resp.e_tag().unwrap().to_string();
    let same_cfg = cfg_resp.distribution_config().unwrap().clone();

    // No-op update: PUT the exact same config back. ETag must not change.
    let noop = cf
        .update_distribution()
        .id(&id)
        .if_match(&if_match)
        .distribution_config(same_cfg.clone())
        .send()
        .await
        .expect("update_distribution noop");
    assert_eq!(
        noop.e_tag().unwrap(),
        etag1.as_str(),
        "no-op UpdateDistribution must not bump the ETag"
    );

    // Real change: flip ViewerProtocolPolicy. ETag must bump.
    let mutated = DistributionConfig::builder()
        .caller_reference(same_cfg.caller_reference())
        .comment(same_cfg.comment())
        .enabled(same_cfg.enabled())
        .origins(same_cfg.origins().unwrap().clone())
        .default_cache_behavior(
            DefaultCacheBehavior::builder()
                .target_origin_id(
                    same_cfg
                        .default_cache_behavior()
                        .unwrap()
                        .target_origin_id(),
                )
                .viewer_protocol_policy(ViewerProtocolPolicy::HttpsOnly)
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
        .unwrap();
    let real = cf
        .update_distribution()
        .id(&id)
        .if_match(noop.e_tag().unwrap())
        .distribution_config(mutated)
        .send()
        .await
        .expect("update_distribution real");
    assert_ne!(
        real.e_tag().unwrap(),
        etag1.as_str(),
        "UpdateDistribution with a real config change must bump the ETag"
    );
    assert_eq!(
        real.distribution().unwrap().status(),
        "InProgress",
        "real UpdateDistribution must flip status back to InProgress"
    );
}
