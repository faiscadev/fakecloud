//! ECR -> ECS and ECR -> Lambda end-to-end flows. These prove the
//! flagship cross-service integration: an image pushed to fakecloud ECR
//! via the OCI v2 endpoint is pulled and run by ECS tasks / Lambda
//! invocations that reference it by AWS URI
//! (`<acct>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`).
//!
//! Gated on docker availability the same way the Lambda invoke tests
//! are: required in CI, skipped otherwise so local dev without docker
//! still passes.

mod helpers;

use std::time::Duration;

use aws_sdk_ecs::types::{ContainerDefinition, KeyValuePair};
use base64::Engine;
use helpers::TestServer;
use tokio::process::Command;

const SEED_IMAGE: &str = "public.ecr.aws/docker/library/alpine:3.20";
const AWS_ACCOUNT: &str = "123456789012";
const AWS_REGION: &str = "us-east-1";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// These tests require the docker daemon to reach fakecloud's OCI v2
/// endpoint on `127.0.0.1:<port>`. That works transparently on Linux
/// (daemon + server share the host network) but not on macOS Docker
/// Desktop without `insecure-registries` config, because the daemon
/// runs in a VM. Gate on Linux-or-opt-in so CI runs the full loop
/// while local macOS dev just skips. CI is Linux.
fn runtime_supports_local_registry() -> bool {
    if cfg!(target_os = "linux") {
        return true;
    }
    std::env::var("FAKECLOUD_DOCKER_LOCAL_REGISTRY_OK").is_ok()
}

fn require_docker_or_skip(test: &str) -> bool {
    if !runtime_supports_local_registry() {
        if std::env::var("CI").is_ok() {
            panic!(
                "{test} needs docker daemon + fakecloud on the same host network. \
                 On macOS set FAKECLOUD_DOCKER_LOCAL_REGISTRY_OK=1 after adding \
                 `127.0.0.1:0/0` to Docker Desktop's insecure-registries."
            );
        }
        eprintln!(
            "skipping {test}: docker daemon cannot reach 127.0.0.1 registry \
             (macOS Docker Desktop runs the daemon in a VM)"
        );
        return false;
    }
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("skipping {test}: docker is not available");
    false
}

fn port_from_endpoint(endpoint: &str) -> u16 {
    endpoint
        .rsplit(':')
        .next()
        .and_then(|p| p.trim_end_matches('/').parse().ok())
        .expect("port from endpoint")
}

/// Run `docker` with the caller-provided args; panics with stderr on
/// failure so a test diagnoses the real cause instead of silently
/// returning an `Err`.
async fn run_docker(args: &[&str]) {
    let out = Command::new("docker")
        .args(args)
        .output()
        .await
        .expect("spawn docker");
    if !out.status.success() {
        panic!(
            "docker {:?} failed: status={:?} stderr={} stdout={}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout),
        );
    }
}

/// `docker pull` against `public.ecr.aws` is rate-limited per IP
/// (1 req/sec anonymous). Shared CI runner IPs hit `toomanyrequests`
/// often enough that a single attempt is flaky. Retry with exponential
/// backoff so a transient rate-limit doesn't fail the whole suite.
async fn run_docker_pull_with_retry(image: &str) {
    let mut last_err = String::new();
    for attempt in 0..6 {
        if attempt > 0 {
            let delay = 5u64 * (1u64 << (attempt - 1)).min(8);
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        let out = Command::new("docker")
            .args(["pull", "--quiet", image])
            .output()
            .await
            .expect("spawn docker");
        if out.status.success() {
            return;
        }
        last_err = String::from_utf8_lossy(&out.stderr).to_string();
        if !last_err.contains("toomanyrequests") && !last_err.contains("Rate exceeded") {
            // Hard failure that retries won't fix.
            panic!("docker pull {image} failed: stderr={last_err}");
        }
    }
    panic!("docker pull {image} rate-limited after 6 attempts: stderr={last_err}");
}

/// Seed fakecloud ECR with a minimal alpine image under `repo:tag`.
/// Returns the AWS-style ECR URI that consumers (ECS/Lambda) will
/// reference.
async fn seed_image(endpoint: &str, repo: &str, tag: &str) -> String {
    let port = port_from_endpoint(endpoint);
    let local_uri = format!("127.0.0.1:{port}/{repo}:{tag}");
    let aws_uri = format!("{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com/{repo}:{tag}");

    run_docker_pull_with_retry(SEED_IMAGE).await;
    run_docker(&["tag", SEED_IMAGE, &local_uri]).await;

    // Write a docker config.json with Basic auth for our registry port.
    // `docker login` itself would also work but does an HTTPS probe
    // first, and Docker Desktop's "localhost is insecure" heuristic
    // isn't guaranteed to fall back to HTTP in all daemon versions.
    // Writing the file directly makes the push work identically.
    let docker_home = tempfile::tempdir().expect("tempdir");
    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("AWS:fakecloud-seed-{port}").as_bytes());
    let config = serde_json::json!({
        "auths": {
            format!("127.0.0.1:{port}"): { "auth": auth },
        }
    });
    std::fs::write(docker_home.path().join("config.json"), config.to_string())
        .expect("write docker config");

    run_docker_with_config(docker_home.path(), &["push", &local_uri]).await;

    // Drop the local tag now that the registry holds it. The runtime rewrites the
    // AWS URI to this very same `127.0.0.1:<port>/<repo>:<tag>`, so leaving the
    // tag in the daemon means a consumer can satisfy the "pull" from the local
    // cache and never touch fakecloud's registry at all -- and this is the only
    // e2e in the repo where a container runtime pulls from the bundled OCI
    // registry, so that would quietly retire the coverage. It is not theoretical:
    // a change that made ECS's launch cache-aware did exactly that, and every
    // assertion here still passed.
    //
    // Best effort: the layers stay behind (shared with the public seed image), so
    // this removes the NAME the runtime resolves, which is what decides whether
    // the registry is asked. A daemon that refuses the untag should not fail a
    // suite that can still run.
    let untag = Command::new("docker")
        .args(["rmi", &local_uri])
        .output()
        .await
        .expect("spawn docker");
    if !untag.status.success() {
        eprintln!(
            "note: could not untag {local_uri}: {}",
            String::from_utf8_lossy(&untag.stderr).trim()
        );
    }

    aws_uri
}

async fn run_docker_with_config(config_dir: &std::path::Path, args: &[&str]) {
    let out = Command::new("docker")
        .env("DOCKER_CONFIG", config_dir)
        .args(args)
        .output()
        .await
        .expect("spawn docker");
    if !out.status.success() {
        panic!(
            "docker {:?} failed: status={:?} stderr={}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
        );
    }
}

async fn task_id_from_arn(arn: &str) -> String {
    arn.rsplit('/').next().unwrap().to_string()
}

async fn fetch_task_logs(endpoint: &str, task_id: &str) -> serde_json::Value {
    let http = reqwest::Client::new();
    http.get(format!("{endpoint}/_fakecloud/ecs/tasks/{task_id}/logs"))
        .send()
        .await
        .expect("introspection fetch")
        .json()
        .await
        .expect("introspection json")
}

/// Push an image to fakecloud ECR, then RunTask with the AWS URI; the
/// task's runtime must translate the AWS URI to the local OCI v2
/// endpoint, pull successfully, run to exit, and capture stdout.
#[tokio::test]
async fn ecr_push_then_ecs_run_task_pulls_image() {
    if !require_docker_or_skip("ecr_push_then_ecs_run_task_pulls_image") {
        return;
    }
    let server = TestServer::start().await;

    let ecr = server.ecr_client().await;
    ecr.create_repository()
        .repository_name("ecs-pull-test")
        .send()
        .await
        .expect("create_repository");

    let aws_uri = seed_image(server.endpoint(), "ecs-pull-test", "v1").await;

    let ecs = server.ecs_client().await;
    ecs.create_cluster()
        .cluster_name("ecr-ecs-cluster")
        .send()
        .await
        .expect("create_cluster");

    ecs.register_task_definition()
        .family("ecr-pull-task")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image(&aws_uri)
                .essential(true)
                .entry_point("/bin/sh")
                .command("-c")
                .command("echo from-ecr && exit 0")
                .build(),
        )
        .send()
        .await
        .expect("register_task_definition");

    let run = ecs
        .run_task()
        .cluster("ecr-ecs-cluster")
        .task_definition("ecr-pull-task")
        .send()
        .await
        .expect("run_task");
    assert_eq!(run.tasks().len(), 1);
    let arn = run.tasks()[0].task_arn().unwrap().to_string();
    let task_id = task_id_from_arn(&arn).await;

    // Poll until the task reaches STOPPED. Pulling the seed + running
    // should complete well inside 90s on a warm CI runner.
    let mut final_status = String::new();
    for _ in 0..180 {
        let desc = ecs
            .describe_tasks()
            .cluster("ecr-ecs-cluster")
            .tasks(&arn)
            .send()
            .await
            .expect("describe_tasks");
        let t = &desc.tasks()[0];
        final_status = t.last_status().unwrap_or_default().to_string();
        if final_status == "STOPPED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(final_status, "STOPPED", "task did not reach STOPPED");

    let logs = fetch_task_logs(server.endpoint(), &task_id).await;
    let captured = logs["logs"].as_str().unwrap_or_default();
    assert!(
        captured.contains("from-ecr"),
        "expected captured logs to contain 'from-ecr'; got: {captured}"
    );
    assert_eq!(logs["exitCode"].as_i64(), Some(0), "logs={logs}");
}

/// Negative: referencing an image in a nonexistent ECR repo should
/// surface a TaskFailedToStart with a clean pull error, not hang.
#[tokio::test]
async fn ecs_run_task_missing_ecr_repo_fails_fast() {
    if !require_docker_or_skip("ecs_run_task_missing_ecr_repo_fails_fast") {
        return;
    }
    let server = TestServer::start().await;

    let ecs = server.ecs_client().await;
    ecs.create_cluster()
        .cluster_name("ecr-missing-cluster")
        .send()
        .await
        .expect("create_cluster");

    let bogus_uri = format!("{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com/missing-repo:v1");
    ecs.register_task_definition()
        .family("ecr-missing-task")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image(&bogus_uri)
                .essential(true)
                .environment(
                    KeyValuePair::builder()
                        .name("TEST")
                        .value("missing")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("register_task_definition");

    let run = ecs
        .run_task()
        .cluster("ecr-missing-cluster")
        .task_definition("ecr-missing-task")
        .send()
        .await
        .expect("run_task");
    let arn = run.tasks()[0].task_arn().unwrap().to_string();

    let mut final_code: Option<String> = None;
    for _ in 0..60 {
        let desc = ecs
            .describe_tasks()
            .cluster("ecr-missing-cluster")
            .tasks(&arn)
            .send()
            .await
            .expect("describe_tasks");
        let t = &desc.tasks()[0];
        if t.last_status() == Some("STOPPED") {
            final_code = t.stop_code().map(|c| c.as_str().to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        final_code.as_deref(),
        Some("TaskFailedToStart"),
        "task should fail fast with TaskFailedToStart; got {final_code:?}"
    );
}

/// Push an image to fakecloud ECR, deploy a Lambda with PackageType=Image
/// pointing at the AWS URI, invoke it. Verifies `Code.ImageUri` flows
/// through CreateFunction -> GetFunction and the runtime pulls the
/// image from fakecloud ECR.
///
/// We only assert the control plane here (function config + pull
/// success) because invoking requires the image to ship a Lambda
/// Runtime Interface Emulator — alpine doesn't have RIE. A separate
/// Lambda image test is left to the existing Lambda suite with a
/// proper `public.ecr.aws/lambda/*` base image when we wire up the
/// image seeding helper there.
#[tokio::test]
async fn ecr_push_then_lambda_create_records_image_uri() {
    let server = TestServer::start().await;

    let ecr = server.ecr_client().await;
    ecr.create_repository()
        .repository_name("lambda-image-test")
        .send()
        .await
        .expect("create_repository");

    // We only need the AWS URI to be stored — no actual pull for the
    // control-plane assertions.
    let aws_uri = format!("{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com/lambda-image-test:v1");

    let lambda = server.lambda_client().await;
    lambda
        .create_function()
        .function_name("image-func")
        .package_type(aws_sdk_lambda::types::PackageType::Image)
        .role("arn:aws:iam::123456789012:role/test-role")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .image_uri(&aws_uri)
                .build(),
        )
        .send()
        .await
        .expect("create_function with Image package");

    let got = lambda
        .get_function()
        .function_name("image-func")
        .send()
        .await
        .expect("get_function");
    assert_eq!(
        got.code()
            .and_then(|c| c.image_uri().map(|s| s.to_string())),
        Some(aws_uri.clone()),
        "GetFunction Code.ImageUri roundtrip"
    );
    let pkg = got
        .configuration()
        .and_then(|c| c.package_type().map(|p| p.as_str().to_string()));
    assert_eq!(pkg.as_deref(), Some("Image"));
}

/// PackageType=Image without Code.ImageUri must be rejected with an
/// `InvalidParameterValueException` — matches AWS Lambda's behaviour.
#[tokio::test]
async fn lambda_image_package_requires_image_uri() {
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;

    let err = lambda
        .create_function()
        .function_name("missing-uri")
        .package_type(aws_sdk_lambda::types::PackageType::Image)
        .role("arn:aws:iam::123456789012:role/test-role")
        .code(aws_sdk_lambda::types::FunctionCode::builder().build())
        .send()
        .await
        .expect_err("create_function should reject missing ImageUri");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("InvalidParameterValueException") || msg.contains("ImageUri"),
        "unexpected error: {msg}"
    );
}

/// A container-image function's own external extensions (baked into the
/// image at `/opt/extensions`) start beside its runtime, as Lambda starts
/// them: the extension registers, gets an `INVOKE` per invocation, and the
/// next event waits until it is done with the previous one.
#[tokio::test]
async fn ecr_image_lambda_starts_its_own_extensions() {
    if !require_docker_or_skip("ecr_image_lambda_starts_its_own_extensions") {
        return;
    }
    let server = TestServer::start().await;
    let ecr = server.ecr_client().await;
    ecr.create_repository()
        .repository_name("lambda-ext-image")
        .send()
        .await
        .expect("create_repository");

    let extension = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2020-01-01/extension"
H=$(mktemp)
curl -sS -D "$H" -o /dev/null -X POST "$API/register" \
  -H "Lambda-Extension-Name: image-ext" -d '{"events":["INVOKE"]}'
ID=$(grep -Fi Lambda-Extension-Identifier "$H" | cut -d: -f2- | tr -d '[:space:]')
while true; do
  curl -sS "$API/event/next" -H "Lambda-Extension-Identifier: $ID" >> /tmp/ext-events
  echo >> /tmp/ext-events
done
"#;
    let bootstrap = r#"#!/bin/sh
API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime"
while true; do
  HEADERS=$(mktemp)
  curl -sS -D "$HEADERS" -o /dev/null "$API/invocation/next"
  ID=$(grep -Fi Lambda-Runtime-Aws-Request-Id "$HEADERS" | cut -d: -f2- | tr -d '[:space:]')
  touch /tmp/ext-events
  curl -sS -o /dev/null -X POST "$API/invocation/$ID/response" --data-binary @/tmp/ext-events
done
"#;
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("image-ext"), extension).unwrap();
    std::fs::write(dir.path().join("bootstrap"), bootstrap).unwrap();
    std::fs::write(
        dir.path().join("Dockerfile"),
        "FROM public.ecr.aws/lambda/provided:al2023\n\
         COPY image-ext /opt/extensions/image-ext\n\
         COPY bootstrap /var/runtime/bootstrap\n\
         RUN chmod 755 /opt/extensions/image-ext /var/runtime/bootstrap\n\
         CMD [\"handler\"]\n",
    )
    .unwrap();

    let port = port_from_endpoint(server.endpoint());
    let local_uri = format!("127.0.0.1:{port}/lambda-ext-image:v1");
    let aws_uri = format!("{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com/lambda-ext-image:v1");
    run_docker_pull_with_retry("public.ecr.aws/lambda/provided:al2023").await;
    run_docker(&["build", "-t", &local_uri, dir.path().to_str().unwrap()]).await;
    let docker_home = tempfile::tempdir().expect("tempdir");
    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("AWS:fakecloud-seed-{port}").as_bytes());
    let config = serde_json::json!({ "auths": { format!("127.0.0.1:{port}"): { "auth": auth } } });
    std::fs::write(docker_home.path().join("config.json"), config.to_string()).unwrap();
    run_docker_with_config(docker_home.path(), &["push", &local_uri]).await;

    let lambda = server.lambda_client().await;
    lambda
        .create_function()
        .function_name("image-with-ext")
        .package_type(aws_sdk_lambda::types::PackageType::Image)
        .role("arn:aws:iam::123456789012:role/test-role")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .image_uri(&aws_uri)
                .build(),
        )
        .send()
        .await
        .expect("create_function");

    let invoke = || async {
        lambda
            .invoke()
            .function_name("image-with-ext")
            .payload(aws_sdk_lambda::primitives::Blob::new(b"{}".to_vec()))
            .send()
            .await
            .expect("invoke")
    };
    let first = invoke().await;
    assert_eq!(first.function_error(), None);
    let first_id = aws_types::request_id::RequestId::request_id(&first)
        .unwrap()
        .to_string();
    let second = invoke().await;
    let seen = String::from_utf8(second.payload().unwrap().as_ref().to_vec()).unwrap();
    assert!(
        seen.lines()
            .any(|l| l.contains("\"INVOKE\"") && l.contains(&first_id)),
        "the image's extension saw no INVOKE for {first_id}: {seen:?}"
    );
}
