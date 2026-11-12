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

/// rustls 0.23 dropped the implicit default CryptoProvider, so every
/// kube TLS connection panics until something installs one. Each test
/// that builds a `Client` directly (without going through
/// `K8sBackend::from_env`) needs to install the provider explicitly;
/// the call is process-wide and idempotent.
fn install_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

async fn client() -> Client {
    install_crypto_provider();
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
    // reap_stale() now filters on the per-service label, so a Pod must
    // carry it to be a reap candidate.
    labels.insert("fakecloud-service".into(), "lambda".into());
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

/// A function invoked through the k8s backend runs on a Pod whose runtime
/// reaches the Runtime API at its own host name (mapped to the fakecloud
/// listener with `hostAliases`): a returned error-shaped object is a
/// success, a raised error is `Unhandled` with the runtime's error object.
/// The listener here is this test's own, on the kind network's gateway, the
/// way an in-cluster fakecloud's Service would be.
#[tokio::test]
async fn invoke_runs_through_the_runtime_api_on_a_pod() {
    use std::io::Write;
    require_test_env();
    let c = client().await;
    ensure_namespace(&c).await;

    let gateway = std::process::Command::new("docker")
        .args([
            "network",
            "inspect",
            "kind",
            "--format",
            "{{range .IPAM.Config}}{{if .Gateway}}{{.Gateway}} {{end}}{{end}}",
        ])
        .output()
        .expect("docker network inspect kind");
    let gateway = String::from_utf8_lossy(&gateway.stdout)
        .split_whitespace()
        .find(|g| g.parse::<std::net::Ipv4Addr>().is_ok())
        .expect("the kind network has an IPv4 gateway")
        .to_string();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    std::env::set_var("FAKECLOUD_K8S_SELF_URL", format!("http://{gateway}:{port}"));
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);

    let code = {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        w.start_file("index.py", zip::write::SimpleFileOptions::default())
            .unwrap();
        w.write_all(
            b"def handler(event, context):\n    if event.get('raise'):\n        raise ValueError('pod boom')\n    return {'errorType': 'X', 'errorMessage': 'returned'}\n",
        )
        .unwrap();
        w.finish().unwrap().into_inner()
    };
    let rt = std::sync::Arc::new(
        fakecloud_lambda::runtime::LambdaRuntime::new_k8s(port, "tok".to_string())
            .await
            .expect("k8s runtime"),
    );
    let api = rt.runtime_api();
    let served_code = code.clone();
    let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let api = api.clone();
        let code = served_code.clone();
        async move {
            if fakecloud_lambda::runtime::RuntimeApi::is_runtime_request(req.headers()) {
                return api.serve(req).await;
            }
            // The init container's code and layer downloads.
            let path = req.uri().path().to_string();
            let body = if path.contains("/_internal/code/") {
                code
            } else {
                Vec::new()
            };
            axum::response::Response::new(axum::body::Body::from(body))
        }
    });
    tokio::spawn(async move { axum::serve(listener, app).await });

    let func: fakecloud_lambda::LambdaFunction = serde_json::from_value(serde_json::json!({
        "function_name": "k8s-rtapi",
        "function_arn": "arn:aws:lambda:us-east-1:000000000000:function:k8s-rtapi",
        "runtime": "python3.12",
        "role": "",
        "handler": "index.handler",
        "description": "",
        "timeout": 30,
        "memory_size": 128,
        "code_sha256": "k8s-rtapi",
        "code_size": code.len(),
        "version": "$LATEST",
        "last_modified": "2020-01-01T00:00:00Z",
        "tags": {},
        "environment": {},
        "architectures": ["x86_64"],
        "package_type": "Zip",
        "code_zip": code,
        "policy": null
    }))
    .unwrap();

    let out = rt.invoke(&func, b"{}", &[]).await.expect("invoke on a Pod");
    rt.stop_all().await;
    assert_eq!(
        out.function_error,
        None,
        "{}",
        String::from_utf8_lossy(&out.payload)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&out.payload).unwrap(),
        serde_json::json!({"errorType": "X", "errorMessage": "returned"})
    );
    let out = rt
        .invoke(&func, br#"{"raise": true}"#, &[])
        .await
        .expect("invoke on a Pod");
    rt.stop_all().await;
    assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
    assert_eq!(out.error_type().as_deref(), Some("ValueError"));
    assert_eq!(out.error_message().as_deref(), Some("pod boom"));
}
