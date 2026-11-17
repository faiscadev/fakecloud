//! ECS container `restartPolicy` and dependency `startTimeout`:
//!
//! 1. A container with a restart policy that exits after running for its
//!    `restartAttemptPeriod` is restarted in place: the task stays RUNNING
//!    with the same container (same `runtimeId`), and the task metadata
//!    endpoint v4 reports its `RestartCount`.
//! 2. A container waiting on `dependsOn` `HEALTHY` gives up once the
//!    upstream has been started for its `startTimeout`, and the task stops
//!    without ever running the dependent.
//! 3. RegisterTaskDefinition rejects a `restartPolicy` outside the model's
//!    limits.

mod helpers;

use std::time::Duration;

use aws_sdk_ecs::types::{
    ContainerCondition, ContainerDefinition, ContainerDependency, ContainerRestartPolicy,
    HealthCheck,
};
use helpers::TestServer;

const ALPINE: &str = "public.ecr.aws/docker/library/alpine:3.20";

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
    eprintln!("skipping {test}: docker is not available");
    false
}

async fn describe(ecs: &aws_sdk_ecs::Client, cluster: &str, arn: &str) -> aws_sdk_ecs::types::Task {
    ecs.describe_tasks()
        .cluster(cluster)
        .tasks(arn)
        .send()
        .await
        .expect("describe_tasks")
        .tasks()[0]
        .clone()
}

#[tokio::test]
async fn ecs_restart_policy_restarts_container_in_place() {
    if !require_docker_or_skip("ecs_restart_policy_restarts_container_in_place") {
        return;
    }
    let server = TestServer::start().await;
    let ecs = server.ecs_client().await;
    ecs.create_cluster()
        .cluster_name("restart-cluster")
        .send()
        .await
        .unwrap();

    // Runs just past the 60s minimum attempt period, then exits 3, which
    // the policy doesn't ignore: ECS restarts it in place.
    ecs.register_task_definition()
        .family("restart-family")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image(ALPINE)
                .essential(true)
                .command("sh")
                .command("-c")
                .command("sleep 62; exit 3")
                .restart_policy(
                    ContainerRestartPolicy::builder()
                        .enabled(true)
                        .ignored_exit_codes(0)
                        .restart_attempt_period(60)
                        .build()
                        .expect("restart policy"),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let arn = ecs
        .run_task()
        .cluster("restart-cluster")
        .task_definition("restart-family")
        .send()
        .await
        .expect("run_task")
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();
    let task_id = arn.rsplit('/').next().unwrap().to_string();

    // Wait for RUNNING and note the container's runtime id.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let runtime_id = loop {
        let task = describe(&ecs, "restart-cluster", &arn).await;
        assert_ne!(
            task.last_status(),
            Some("STOPPED"),
            "task stopped before running: {:?}",
            task.stopped_reason()
        );
        if task.last_status() == Some("RUNNING") {
            break task.containers()[0]
                .runtime_id()
                .expect("runtimeId")
                .to_string();
        }
        assert!(std::time::Instant::now() < deadline, "task never ran");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    let http = reqwest::Client::new();
    let metadata_url = format!("{}/_fakecloud/ecs/v4/{task_id}", server.endpoint());
    let restart_count = || async {
        let body: serde_json::Value = http
            .get(&metadata_url)
            .send()
            .await
            .expect("task metadata")
            .json()
            .await
            .expect("task metadata json");
        body["Containers"][0]["RestartCount"].as_u64()
    };
    assert_eq!(restart_count().await, Some(0), "RestartCount starts at 0");

    // The exit lands ~62s after start; the restart follows within a poll.
    let deadline = std::time::Instant::now() + Duration::from_secs(150);
    loop {
        let task = describe(&ecs, "restart-cluster", &arn).await;
        assert_ne!(
            task.last_status(),
            Some("STOPPED"),
            "the restart policy should have kept the task running: {:?}",
            task.stopped_reason()
        );
        if restart_count().await == Some(1) {
            assert_eq!(task.last_status(), Some("RUNNING"));
            let c = &task.containers()[0];
            assert_eq!(c.last_status(), Some("RUNNING"));
            assert_eq!(
                c.runtime_id(),
                Some(runtime_id.as_str()),
                "restarted in place, not replaced"
            );
            assert_eq!(c.exit_code(), None);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "container was never restarted"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let _ = ecs
        .stop_task()
        .cluster("restart-cluster")
        .task(&arn)
        .reason("e2e-cleanup")
        .send()
        .await;
}

#[tokio::test]
async fn ecs_depends_on_healthy_gives_up_after_start_timeout() {
    if !require_docker_or_skip("ecs_depends_on_healthy_gives_up_after_start_timeout") {
        return;
    }
    let server = TestServer::start().await;
    let ecs = server.ecs_client().await;
    ecs.create_cluster()
        .cluster_name("start-timeout-cluster")
        .send()
        .await
        .unwrap();

    // `db` never becomes healthy; `app` waits on HEALTHY for db's 10s
    // startTimeout, then gives up.
    ecs.register_task_definition()
        .family("start-timeout-family")
        .container_definitions(
            ContainerDefinition::builder()
                .name("db")
                .image(ALPINE)
                .essential(false)
                .command("sh")
                .command("-c")
                .command("sleep 3600")
                .start_timeout(10)
                .health_check(
                    HealthCheck::builder()
                        .command("CMD-SHELL")
                        .command("exit 1")
                        .interval(5)
                        .timeout(2)
                        .retries(1)
                        .build()
                        .expect("health check"),
                )
                .build(),
        )
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image(ALPINE)
                .essential(true)
                .command("sh")
                .command("-c")
                .command("sleep 3600")
                .depends_on(
                    ContainerDependency::builder()
                        .container_name("db")
                        .condition(ContainerCondition::Healthy)
                        .build()
                        .expect("dep"),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let arn = ecs
        .run_task()
        .cluster("start-timeout-cluster")
        .task_definition("start-timeout-family")
        .send()
        .await
        .expect("run_task")
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let task = loop {
        let task = describe(&ecs, "start-timeout-cluster", &arn).await;
        let app = task
            .containers()
            .iter()
            .find(|c| c.name() == Some("app"))
            .unwrap();
        assert_ne!(
            app.last_status(),
            Some("RUNNING"),
            "app started although its dependency never became healthy"
        );
        if task.last_status() == Some("STOPPED") {
            break task;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "task did not stop after the dependency's startTimeout"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        task.stop_code(),
        Some(&aws_sdk_ecs::types::TaskStopCode::TaskFailedToStart)
    );
    let reason = task.stopped_reason().unwrap_or_default();
    assert!(
        reason.contains("startTimeout") && reason.contains("db") && reason.contains("HEALTHY"),
        "unexpected stoppedReason: {reason}"
    );
    // It gave up no earlier than the startTimeout after db started.
    let started = task.created_at().expect("createdAt");
    let stopped = task.stopped_at().expect("stoppedAt");
    assert!(
        stopped.secs() - started.secs() >= 10,
        "gave up before the startTimeout"
    );
}

#[tokio::test]
async fn ecs_register_task_definition_validates_restart_policy() {
    let server = TestServer::start().await;
    let ecs = server.ecs_client().await;
    let register = |period: i32| {
        ecs.register_task_definition()
            .family("restart-validate")
            .container_definitions(
                ContainerDefinition::builder()
                    .name("app")
                    .image(ALPINE)
                    .restart_policy(
                        ContainerRestartPolicy::builder()
                            .enabled(true)
                            .restart_attempt_period(period)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send()
    };
    for bad in [59, 1801] {
        let err = register(bad).await.expect_err("out-of-range period");
        let svc = err.into_service_error();
        assert!(svc.is_client_exception(), "{svc:?}");
    }
    let ok = register(60).await.expect("minimum period accepted");
    let td = ok.task_definition().unwrap();
    let policy = td.container_definitions()[0].restart_policy().unwrap();
    assert!(policy.enabled());
    assert_eq!(policy.restart_attempt_period(), Some(60));
}
