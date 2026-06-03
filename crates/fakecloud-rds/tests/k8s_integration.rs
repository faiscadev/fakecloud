//! Opt-in Kubernetes integration tests for the RDS k8s backend.
//!
//! Needs a real cluster (a local `kind` cluster works) with a Postgres
//! image loaded, plus a valid kubeconfig. Gated behind the
//! `k8s-integration` feature.
//!
//! Per `feedback_tests_never_silently_skip`: with the feature on, a
//! missing `FAKECLOUD_K8S_TEST=1` / unreachable cluster **panics** rather
//! than silently passing.
//!
//! Pod IPs aren't routable from the host on `kind`, so these drive the DB
//! through the kube `exec` subresource (the same path `dump_database` /
//! `restore_database` / `read_file` use) rather than connecting to 5432
//! directly. A stock `postgres:16-alpine` stands in for the bridge image
//! to keep CI fast — the assertions are about the k8s plumbing, not the
//! aws_lambda/aws_s3 extensions.
//!
//! Run with:
//! ```sh
//! kind create cluster --name fakecloud-test
//! docker pull postgres:16-alpine && kind load docker-image postgres:16-alpine --name fakecloud-test
//! FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-rds \
//!     --features k8s-integration --test k8s_integration -- --test-threads=1
//! ```

#![cfg(feature = "k8s-integration")]

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use fakecloud_k8s::{labels, K8sClient};
use fakecloud_rds::runtime::RdsRuntime;

const TEST_NS: &str = "fakecloud-rds-test";

fn require_test_env() {
    if std::env::var("FAKECLOUD_K8S_TEST").is_err() {
        panic!(
            "FAKECLOUD_K8S_TEST not set — refusing to silently skip k8s integration tests.\n\
             kind create cluster --name fakecloud-test\n  \
             kind load docker-image postgres:16-alpine --name fakecloud-test\n  \
             FAKECLOUD_K8S_TEST=1 cargo test -p fakecloud-rds \\\n      \
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

fn postgres_pod(name: &str) -> Pod {
    let mut l = BTreeMap::new();
    l.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    l.insert(labels::SERVICE.to_string(), "rds".to_string());
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
                name: "db".into(),
                image: Some("postgres:16-alpine".into()),
                env: Some(vec![EnvVar {
                    name: "POSTGRES_PASSWORD".into(),
                    value: Some("secret".into()),
                    value_from: None,
                }]),
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
        "http://fakecloud.fakecloud-rds-test.svc.cluster.local:4566",
    );
    std::env::set_var("FAKECLOUD_K8S_NAMESPACE", TEST_NS);
    let rt = RdsRuntime::new_k8s(4566).await.expect("new_k8s");
    assert_eq!(rt.cli_name(), "kubernetes");
    rt.reap_stale().await;
}

#[tokio::test]
async fn postgres_pod_exec_readiness_query_and_dump() {
    require_test_env();
    ensure_namespace().await;
    let c = client().await;
    let name = "fakecloud-rds-it-pg";
    c.delete_pod(name).await;
    c.create_pod(&postgres_pod(name))
        .await
        .expect("create pg pod");
    c.wait_for_pod_ip(name, Duration::from_secs(120))
        .await
        .expect("pg pod Running");

    // Readiness via exec pg_isready (API-server path, no pod-IP routing).
    let mut ready = false;
    for _ in 0..60 {
        if let Ok(out) = c
            .exec(name, Some("db"), &["pg_isready", "-U", "postgres"])
            .await
        {
            if out.success() {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    assert!(ready, "postgres did not become ready via pg_isready");

    // A query through exec psql.
    let q = c
        .exec(
            name,
            Some("db"),
            &["psql", "-U", "postgres", "-tAc", "SELECT 1"],
        )
        .await
        .expect("psql query");
    assert!(q.success(), "psql failed: {}", q.stderr);
    assert!(q.stdout_str().contains('1'));

    // The dump path RDS uses: pg_dump via exec produces a non-empty dump.
    let dump = c
        .exec(
            name,
            Some("db"),
            &[
                "pg_dump",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "--no-password",
            ],
        )
        .await
        .expect("pg_dump");
    assert!(dump.success(), "pg_dump failed: {}", dump.stderr);
    assert!(
        dump.stdout_str().contains("PostgreSQL database dump"),
        "pg_dump output missing header"
    );

    c.delete_pod(name).await;
}

#[tokio::test]
async fn reap_stale_deletes_foreign_instance_pods() {
    require_test_env();
    ensure_namespace().await;
    let c = client().await;
    let name = "fakecloud-rds-it-foreign";
    c.delete_pod(name).await;
    let mut pod = postgres_pod(name);
    pod.metadata
        .labels
        .as_mut()
        .unwrap()
        .insert(labels::INSTANCE.to_string(), "fakecloud-99999".to_string());
    c.create_pod(&pod).await.expect("create foreign pod");

    let reaped = c.reap_stale("rds").await;
    assert!(reaped >= 1, "expected to reap the foreign pod");

    for _ in 0..60 {
        if c.pods().get_opt(name).await.unwrap().is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("foreign pod still present after reap");
}
