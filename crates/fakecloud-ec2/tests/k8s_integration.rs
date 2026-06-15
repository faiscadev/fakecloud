//! Opt-in Kubernetes integration tests for the EC2 instance k8s backend.
//!
//! These need a real cluster (a local `kind` cluster works) with the
//! `busybox:1.36` image loaded, plus a valid kubeconfig. Gated behind the
//! `k8s-integration` feature so a casual `cargo test` doesn't try to talk to
//! a cluster that isn't there.
//!
//! Per `feedback_tests_never_silently_skip`: with the feature on, a missing
//! `FAKECLOUD_K8S_TEST=1` / unreachable cluster **panics** rather than
//! silently passing.
//!
//! Pod IPs aren't routable from the host on `kind`, so these tests verify the
//! instance container via the kube `exec` subresource (which goes through the
//! API server) instead of connecting to the Pod directly.
//!
//! Run with:
//! ```sh
//! kind create cluster --name fakecloud-test
//! docker pull busybox:1.36 && kind load docker-image busybox:1.36 --name fakecloud-test
//! FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-ec2 \
//!     --features k8s-integration --test k8s_integration -- --test-threads=1
//! ```

#![cfg(feature = "k8s-integration")]

use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use fakecloud_ec2::runtime::Ec2Runtime;
use fakecloud_k8s::K8sClient;

const TEST_NS: &str = "fakecloud-i-test";
/// base64 of `echo ran > /tmp/marker\n` — the user-data each instance runs.
const USER_DATA_B64: &str = "ZWNobyByYW4gPiAvdG1wL21hcmtlcgo=";

fn require_test_env() {
    if std::env::var("FAKECLOUD_K8S_TEST").is_err() {
        panic!(
            "FAKECLOUD_K8S_TEST not set — refusing to silently skip k8s integration tests.\n\
             Set FAKECLOUD_K8S_TEST=1 and point KUBECONFIG at a cluster, e.g.:\n  \
             kind create cluster --name fakecloud-test\n  \
             kind load docker-image busybox:1.36 --name fakecloud-test\n  \
             FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-ec2 \\\n      \
                 --features k8s-integration --test k8s_integration -- --test-threads=1"
        );
    }
}

async fn client() -> K8sClient {
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

fn k8s_runtime_env() {
    std::env::set_var("FAKECLOUD_EC2_DEFAULT_IMAGE", "busybox:1.36");
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    std::env::set_var(
        "FAKECLOUD_K8S_SELF_URL",
        "http://fakecloud.fakecloud-i-test.svc.cluster.local:4566",
    );
}

/// Poll `cat /tmp/marker` inside the instance container via exec. Returns the
/// trimmed stdout, or `None` if the exec fails (e.g. the Pod is gone).
async fn read_marker(c: &K8sClient, pod: &str) -> Option<String> {
    let out = c
        .exec(pod, Some("instance"), &["cat", "/tmp/marker"])
        .await
        .ok()?;
    if out.success() {
        Some(out.stdout_str().trim().to_string())
    } else {
        None
    }
}

#[tokio::test]
async fn precondition_env_must_be_set() {
    require_test_env();
}

#[tokio::test]
async fn new_k8s_constructs_and_reports_kubernetes() {
    require_test_env();
    k8s_runtime_env();
    let rt = Ec2Runtime::new_k8s(4566).await.expect("new_k8s");
    assert_eq!(rt.cli_name(), "kubernetes");
    // reap_stale must not panic against a real cluster.
    rt.reap_stale().await;
}

#[tokio::test]
async fn instance_lifecycle_boots_pod_runs_user_data_and_recreates_on_start() {
    require_test_env();
    ensure_namespace().await;
    k8s_runtime_env();

    let rt = Ec2Runtime::new_k8s(4566).await.expect("new_k8s");
    let c = client().await;
    let instance_id = "i-0k8slifecycle";

    // RunInstances boots a Pod and runs user-data at boot.
    let running = rt
        .run_instance(instance_id, Some(USER_DATA_B64))
        .await
        .expect("run_instance");
    let pod = running.container_id.clone();
    assert!(!running.private_ip.is_empty(), "pod should report an IP");

    let mut marker = None;
    for _ in 0..40 {
        marker = read_marker(&c, &pod).await;
        if marker.as_deref() == Some("ran") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        marker.as_deref(),
        Some("ran"),
        "user-data did not run in Pod"
    );

    // StopInstances deletes the Pod.
    rt.stop_instance(instance_id).await;
    let mut gone = false;
    for _ in 0..40 {
        if read_marker(&c, &pod).await.is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(gone, "Pod should be deleted after StopInstances");

    // StartInstances recreates the Pod (same name) and re-runs user-data.
    let new_ip = rt.start_instance(instance_id).await;
    assert!(
        new_ip.is_some(),
        "start should report the recreated Pod's IP"
    );
    let mut restarted = None;
    for _ in 0..40 {
        restarted = read_marker(&c, &pod).await;
        if restarted.as_deref() == Some("ran") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        restarted.as_deref(),
        Some("ran"),
        "Pod should be running again after StartInstances"
    );

    // TerminateInstances removes the Pod for good.
    rt.terminate_instance(instance_id).await;
    let mut removed = false;
    for _ in 0..40 {
        if read_marker(&c, &pod).await.is_none() {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(removed, "Pod should be gone after TerminateInstances");
}
