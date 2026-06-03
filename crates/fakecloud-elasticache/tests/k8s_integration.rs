//! Opt-in Kubernetes integration tests for the ElastiCache k8s backend.
//!
//! These need a real cluster (a local `kind` cluster works) with the
//! `redis:7-alpine` image loaded, plus a valid kubeconfig. Gated behind
//! the `k8s-integration` feature so a casual `cargo test` doesn't try to
//! talk to a cluster that isn't there.
//!
//! Per `feedback_tests_never_silently_skip`: with the feature on, a
//! missing `FAKECLOUD_K8S_TEST=1` / unreachable cluster **panics** rather
//! than silently passing.
//!
//! Pod IPs aren't routable from the host on `kind`, so these tests drive
//! the cache via the kube `exec` subresource (which goes through the API
//! server) — exactly the path `exec_redis` / `dump_rdb` use — instead of
//! connecting to the Redis port directly.
//!
//! Run with:
//! ```sh
//! kind create cluster --name fakecloud-test
//! docker pull redis:7-alpine && kind load docker-image redis:7-alpine --name fakecloud-test
//! FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-elasticache \
//!     --features k8s-integration --test k8s_integration -- --test-threads=1
//! ```

#![cfg(feature = "k8s-integration")]

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use fakecloud_elasticache::runtime::ElastiCacheRuntime;
use fakecloud_k8s::{labels, K8sClient};

const TEST_NS: &str = "fakecloud-ec-test";

fn require_test_env() {
    if std::env::var("FAKECLOUD_K8S_TEST").is_err() {
        panic!(
            "FAKECLOUD_K8S_TEST not set — refusing to silently skip k8s integration tests.\n\
             Set FAKECLOUD_K8S_TEST=1 and point KUBECONFIG at a cluster, e.g.:\n  \
             kind create cluster --name fakecloud-test\n  \
             kind load docker-image redis:7-alpine --name fakecloud-test\n  \
             FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-elasticache \\\n      \
                 --features k8s-integration --test k8s_integration -- --test-threads=1"
        );
    }
}

async fn client() -> K8sClient {
    // connect() installs the rustls CryptoProvider and reads kubeconfig.
    K8sClient::connect(TEST_NS.to_string())
        .await
        .expect("connect to cluster — set KUBECONFIG or run inside a cluster")
}

async fn ensure_namespace() {
    use k8s_openapi::api::core::v1::Namespace;
    use kube::api::{Api, PostParams};
    let c = K8sClient::connect("default".to_string()).await.unwrap();
    let api: Api<Namespace> = Api::all(c.client().clone());
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some(TEST_NS.into()),
            ..Default::default()
        },
        ..Default::default()
    };
    match api.create(&PostParams::default(), &ns).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => panic!("create test namespace: {e}"),
    }
}

fn redis_pod(name: &str) -> Pod {
    let mut l = BTreeMap::new();
    l.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    l.insert(labels::SERVICE.to_string(), "elasticache".to_string());
    l.insert(labels::INSTANCE.to_string(), labels::instance_id());
    Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(TEST_NS.into()),
            labels: Some(l),
            ..Default::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".into()),
            containers: vec![Container {
                name: "cache".into(),
                image: Some("redis:7-alpine".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn precondition_env_must_be_set() {
    require_test_env();
}

#[tokio::test]
async fn new_k8s_constructs_and_reports_kubernetes() {
    require_test_env();
    std::env::set_var(
        "FAKECLOUD_K8S_SELF_URL",
        "http://fakecloud.fakecloud-ec-test.svc.cluster.local:4566",
    );
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    let rt = ElastiCacheRuntime::new_k8s(4566, "tok".into())
        .await
        .expect("new_k8s");
    assert_eq!(rt.cli_name(), "kubernetes");
    assert!(rt.pending_rdb().is_some());
    // reap_stale must not panic against a real cluster.
    rt.reap_stale().await;
}

#[tokio::test]
async fn redis_pod_exec_ping_set_get_and_save() {
    require_test_env();
    ensure_namespace().await;
    let c = client().await;
    let name = "fakecloud-ec-it-redis";
    c.delete_pod(name).await;

    c.create_pod(&redis_pod(name))
        .await
        .expect("create redis pod");
    c.wait_for_pod_ip(name, Duration::from_secs(90))
        .await
        .expect("redis pod Running");

    // redis-server inside the pod needs a moment to accept commands even
    // after the pod is Running. Retry the PING via exec (API-server path,
    // independent of pod-IP routability).
    let mut pinged = false;
    for _ in 0..20 {
        if let Ok(out) = c.exec(name, Some("cache"), &["redis-cli", "PING"]).await {
            if out.success() && out.stdout_str().contains("PONG") {
                pinged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(pinged, "redis did not answer PING via exec");

    let set = c
        .exec(name, Some("cache"), &["redis-cli", "SET", "k", "v"])
        .await
        .expect("SET");
    assert!(set.success(), "SET failed: {}", set.stderr);
    let get = c
        .exec(name, Some("cache"), &["redis-cli", "GET", "k"])
        .await
        .expect("GET");
    assert!(
        get.stdout_str().contains('v'),
        "GET returned {:?}",
        get.stdout_str()
    );

    // The dump path: SAVE then read /data/dump.rdb out — must be non-empty
    // and start with the RDB magic ("REDIS").
    let save = c
        .exec(name, Some("cache"), &["redis-cli", "SAVE"])
        .await
        .expect("SAVE");
    assert!(save.success(), "SAVE failed: {}", save.stderr);
    let cat = c
        .exec(name, Some("cache"), &["cat", "/data/dump.rdb"])
        .await
        .expect("cat dump.rdb");
    assert!(cat.success(), "cat failed: {}", cat.stderr);
    assert!(
        cat.stdout.starts_with(b"REDIS"),
        "dump.rdb should start with the RDB magic, got {} bytes",
        cat.stdout.len()
    );

    c.delete_pod(name).await;
}

#[tokio::test]
async fn reap_stale_deletes_foreign_instance_pods() {
    require_test_env();
    ensure_namespace().await;
    let c = client().await;

    // A pod labelled as belonging to a *different* process.
    let name = "fakecloud-ec-it-foreign";
    c.delete_pod(name).await;
    let mut pod = redis_pod(name);
    pod.metadata
        .labels
        .as_mut()
        .unwrap()
        .insert(labels::INSTANCE.to_string(), "fakecloud-99999".to_string());
    c.create_pod(&pod).await.expect("create foreign pod");

    // reap_stale runs against the *current* instance id, so the foreign
    // pod is a reap candidate.
    let reaped = c.reap_stale("elasticache").await;
    assert!(reaped >= 1, "expected to reap the foreign pod");

    for _ in 0..60 {
        if c.pods().get_opt(name).await.unwrap().is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("foreign pod still present after reap");
}
