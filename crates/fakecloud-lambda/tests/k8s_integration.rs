//! Opt-in K8s integration tests for the Lambda Kubernetes backend.
//!
//! These tests need a real cluster (a local `kind` cluster works) and
//! a valid kubeconfig. They are gated behind the `k8s-integration`
//! feature so a casual `cargo test` doesn't try to talk to a cluster
//! that isn't there.
//!
//! Per `feedback_tests_never_silently_skip`: when the feature is on,
//! missing toolchain (no `FAKECLOUD_K8S_TEST=1`, no reachable cluster)
//! must **panic with a clear message**, not silently pass. Skipping a
//! test that the operator opted into is a worse failure mode than a
//! red bar — they at least see the red bar.
//!
//! Run with:
//! ```sh
//! kind create cluster --name fakecloud-test
//! FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-lambda \
//!     --features k8s-integration --test k8s_integration
//! ```

#![cfg(feature = "k8s-integration")]

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::Client;

use fakecloud_lambda::runtime::{K8sBackend, LambdaBackend};

const TEST_NS: &str = "fakecloud-k8s-test";

fn require_test_env() {
    if std::env::var("FAKECLOUD_K8S_TEST").is_err() {
        panic!(
            "FAKECLOUD_K8S_TEST not set — refusing to silently skip K8s integration tests.\n\
             Set FAKECLOUD_K8S_TEST=1 and point KUBECONFIG at a cluster, e.g.:\n  \
             kind create cluster --name fakecloud-test\n  \
             FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-lambda \\\n      \
                 --features k8s-integration --test k8s_integration"
        );
    }
}

async fn client() -> Client {
    Client::try_default()
        .await
        .expect("kube::Client::try_default failed — set KUBECONFIG or run inside a cluster")
}

async fn ensure_namespace(c: &Client) {
    use k8s_openapi::api::core::v1::Namespace;
    let api: Api<Namespace> = Api::all(c.clone());
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
        Err(e) => panic!("failed to create test namespace: {e}"),
    }
}

fn dummy_pod(name: &str, instance_label: &str) -> Pod {
    let mut labels = BTreeMap::new();
    labels.insert("fakecloud-managed-by".into(), "fakecloud".into());
    labels.insert("fakecloud-instance".into(), instance_label.into());
    labels.insert("fakecloud-lambda".into(), "test-fn".into());
    Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(TEST_NS.into()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".into()),
            containers: vec![Container {
                name: "pause".into(),
                // gcr.io/google_containers/pause is the canonical do-nothing
                // sleep-forever image; tiny and always reachable.
                image: Some("registry.k8s.io/pause:3.10".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn wait_for_pod_gone(api: &Api<Pod>, name: &str, max_secs: u64) {
    for _ in 0..(max_secs * 2) {
        match api.get_opt(name).await {
            Ok(None) => return,
            Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(500)).await,
            Err(e) => panic!("error polling pod {name}: {e}"),
        }
    }
    panic!("pod {name} still exists after {max_secs}s");
}

#[tokio::test]
async fn precondition_env_must_be_set() {
    require_test_env();
}

#[tokio::test]
async fn k8s_client_connects() {
    require_test_env();
    let c = client().await;
    use k8s_openapi::api::core::v1::Node;
    let api: Api<Node> = Api::all(c);
    let nodes = api
        .list(&ListParams::default())
        .await
        .expect("list nodes failed");
    assert!(!nodes.items.is_empty(), "cluster has no nodes");
}

#[tokio::test]
async fn from_env_initializes_backend() {
    require_test_env();
    let _saved = std::env::var("FAKECLOUD_K8S_SELF_URL").ok();
    // SAFETY: tests in this file are mutually serializable via the
    // shared cluster; we set required env then construct.
    std::env::set_var(
        "FAKECLOUD_K8S_SELF_URL",
        "http://fakecloud.fakecloud-k8s-test.svc.cluster.local:4566",
    );
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    let backend = K8sBackend::from_env(4566, "test-token".to_string())
        .await
        .expect("K8sBackend::from_env failed");
    assert_eq!(backend.name(), "kubernetes");
}

#[tokio::test]
async fn reap_stale_deletes_foreign_instance_pods() {
    require_test_env();
    let c = client().await;
    ensure_namespace(&c).await;
    let api: Api<Pod> = Api::namespaced(c.clone(), TEST_NS);

    let foreign = "fakecloud-reap-foreign";
    // Clean up from any prior failed run.
    let _ = api.delete(foreign, &DeleteParams::default()).await;
    wait_for_pod_gone(&api, foreign, 30).await;

    api.create(
        &PostParams::default(),
        &dummy_pod(foreign, "fakecloud-99999"),
    )
    .await
    .expect("create foreign pod");

    std::env::set_var(
        "FAKECLOUD_K8S_SELF_URL",
        "http://fakecloud.fakecloud-k8s-test.svc.cluster.local:4566",
    );
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    let backend = K8sBackend::from_env(4566, "tok".to_string())
        .await
        .expect("K8sBackend::from_env");

    backend.reap_stale().await;

    wait_for_pod_gone(&api, foreign, 30).await;
}

#[tokio::test]
async fn reap_stale_preserves_own_instance_pods() {
    require_test_env();
    let c = client().await;
    ensure_namespace(&c).await;
    let api: Api<Pod> = Api::namespaced(c.clone(), TEST_NS);

    let own_instance = format!("fakecloud-{}", std::process::id());
    let mine = "fakecloud-reap-mine";

    let _ = api.delete(mine, &DeleteParams::default()).await;
    wait_for_pod_gone(&api, mine, 30).await;

    api.create(&PostParams::default(), &dummy_pod(mine, &own_instance))
        .await
        .expect("create own pod");

    std::env::set_var(
        "FAKECLOUD_K8S_SELF_URL",
        "http://fakecloud.fakecloud-k8s-test.svc.cluster.local:4566",
    );
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    let backend = K8sBackend::from_env(4566, "tok".to_string())
        .await
        .expect("K8sBackend::from_env");

    backend.reap_stale().await;

    let still_there = api
        .get_opt(mine)
        .await
        .expect("get own pod after reap")
        .is_some();
    // Clean up regardless of assertion outcome.
    let _ = api.delete(mine, &DeleteParams::default()).await;
    wait_for_pod_gone(&api, mine, 30).await;

    assert!(
        still_there,
        "reap_stale must preserve pods labeled with current fakecloud-instance"
    );
}

#[tokio::test]
async fn terminate_pod_handle_is_idempotent() {
    require_test_env();
    let c = client().await;
    ensure_namespace(&c).await;
    let api: Api<Pod> = Api::namespaced(c.clone(), TEST_NS);

    let name = "fakecloud-terminate-idem";
    let _ = api.delete(name, &DeleteParams::default()).await;
    wait_for_pod_gone(&api, name, 30).await;

    std::env::set_var(
        "FAKECLOUD_K8S_SELF_URL",
        "http://fakecloud.fakecloud-k8s-test.svc.cluster.local:4566",
    );
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    let backend = K8sBackend::from_env(4566, "tok".to_string())
        .await
        .expect("K8sBackend::from_env");

    let handle = fakecloud_lambda::runtime::BackendHandle::Pod {
        namespace: TEST_NS.into(),
        name: name.into(),
    };
    // Delete-not-found must not panic.
    backend.terminate(&handle).await;
    backend.terminate(&handle).await;
}
