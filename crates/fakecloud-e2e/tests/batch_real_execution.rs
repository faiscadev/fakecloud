//! AWS Batch SubmitJob runs a REAL container on the ECS engine and drives the
//! job status off its real exit code — the wedge against every rival, whose
//! Batch compute is faked (ministack jumps to SUCCEEDED with no container).
//!
//! Docker-gated: requires a working docker daemon (mirrors the EC2/ECS
//! real-runtime tests). Panics in CI if docker is missing, skips locally.

mod helpers;

use std::time::Duration;

use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

const IMAGE: &str = "public.ecr.aws/docker/library/alpine:3.20";

async fn run_job(batch: &aws_sdk_batch::Client, name: &str, command: Vec<&str>) -> String {
    batch
        .create_compute_environment()
        .compute_environment_name(format!("{name}-ce"))
        .r#type(aws_sdk_batch::types::CeType::Managed)
        .send()
        .await
        .expect("create CE");
    batch
        .create_job_queue()
        .job_queue_name(format!("{name}-q"))
        .priority(1)
        .compute_environment_order(
            aws_sdk_batch::types::ComputeEnvironmentOrder::builder()
                .order(1)
                .compute_environment(format!("{name}-ce"))
                .build(),
        )
        .send()
        .await
        .expect("create JQ");
    batch
        .register_job_definition()
        .job_definition_name(format!("{name}-jd"))
        .r#type(aws_sdk_batch::types::JobDefinitionType::Container)
        .container_properties(
            aws_sdk_batch::types::ContainerProperties::builder()
                .image(IMAGE)
                .set_command(Some(command.iter().map(|s| s.to_string()).collect()))
                .resource_requirements(
                    aws_sdk_batch::types::ResourceRequirement::builder()
                        .r#type(aws_sdk_batch::types::ResourceType::Vcpu)
                        .value("1")
                        .build(),
                )
                .resource_requirements(
                    aws_sdk_batch::types::ResourceRequirement::builder()
                        .r#type(aws_sdk_batch::types::ResourceType::Memory)
                        .value("128")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("register JD");
    let job = batch
        .submit_job()
        .job_name(format!("{name}-job"))
        .job_queue(format!("{name}-q"))
        .job_definition(format!("{name}-jd"))
        .send()
        .await
        .expect("submit job");
    job.job_id().unwrap().to_string()
}

async fn wait_terminal(batch: &aws_sdk_batch::Client, job_id: &str) -> String {
    for _ in 0..120 {
        let d = batch.describe_jobs().jobs(job_id).send().await.unwrap();
        let status = d.jobs()[0]
            .status()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        if status == "SUCCEEDED" || status == "FAILED" {
            return status;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("job {job_id} never reached a terminal status");
}

#[tokio::test]
async fn submit_job_runs_real_container_and_succeeds() {
    if !require_docker_or_skip("submit_job_runs_real_container_and_succeeds") {
        return;
    }
    let s = TestServer::start().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);
    let job_id = run_job(&batch, "ok", vec!["sh", "-c", "exit 0"]).await;
    assert_eq!(wait_terminal(&batch, &job_id).await, "SUCCEEDED");
    let d = batch.describe_jobs().jobs(&job_id).send().await.unwrap();
    assert_eq!(d.jobs()[0].container().and_then(|c| c.exit_code()), Some(0));
}

#[tokio::test]
async fn submit_job_failing_container_fails_the_job() {
    if !require_docker_or_skip("submit_job_failing_container_fails_the_job") {
        return;
    }
    let s = TestServer::start().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);
    let job_id = run_job(&batch, "bad", vec!["sh", "-c", "exit 7"]).await;
    assert_eq!(wait_terminal(&batch, &job_id).await, "FAILED");
    let d = batch.describe_jobs().jobs(&job_id).send().await.unwrap();
    assert_eq!(d.jobs()[0].container().and_then(|c| c.exit_code()), Some(7));
}

#[tokio::test]
async fn depends_on_job_waits_for_its_dependency() {
    if !require_docker_or_skip("depends_on_job_waits_for_its_dependency") {
        return;
    }
    let s = TestServer::start().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);
    // run_job sets up CE/JQ/JD "dep-*" and submits job A (sleeps then exits 0).
    let a = run_job(&batch, "dep", vec!["sh", "-c", "sleep 3; exit 0"]).await;
    let b = batch
        .submit_job()
        .job_name("dep-b")
        .job_queue("dep-q")
        .job_definition("dep-jd")
        .depends_on(
            aws_sdk_batch::types::JobDependency::builder()
                .job_id(&a)
                .build(),
        )
        .send()
        .await
        .expect("submit B")
        .job_id()
        .unwrap()
        .to_string();

    // While A is still running, B must NOT be RUNNING/SUCCEEDED yet.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let early = batch.describe_jobs().jobs(&b).send().await.unwrap();
    let b_early = early.jobs()[0].status().map(|s| s.as_str()).unwrap_or("");
    assert!(
        b_early == "PENDING" || b_early == "SUBMITTED",
        "B must wait for A, was {b_early}"
    );

    assert_eq!(wait_terminal(&batch, &a).await, "SUCCEEDED");
    assert_eq!(wait_terminal(&batch, &b).await, "SUCCEEDED");
}

#[tokio::test]
async fn array_job_runs_every_child_and_parent_succeeds() {
    if !require_docker_or_skip("array_job_runs_every_child_and_parent_succeeds") {
        return;
    }
    let s = TestServer::start().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);
    // Reuse run_job's setup (CE/JQ/JD with an always-succeed container) but
    // submit as an array of 3.
    let _ = run_job(&batch, "arr", vec!["sh", "-c", "exit 0"]).await;
    let parent = batch
        .submit_job()
        .job_name("arr-array")
        .job_queue("arr-q")
        .job_definition("arr-jd")
        .array_properties(
            aws_sdk_batch::types::ArrayProperties::builder()
                .size(3)
                .build(),
        )
        .send()
        .await
        .expect("submit array job")
        .job_id()
        .unwrap()
        .to_string();

    assert_eq!(wait_terminal(&batch, &parent).await, "SUCCEEDED");
    let d = batch.describe_jobs().jobs(&parent).send().await.unwrap();
    let summary = d.jobs()[0]
        .array_properties()
        .and_then(|a| a.status_summary())
        .expect("array statusSummary");
    assert_eq!(summary.get("SUCCEEDED"), Some(&3));
}
