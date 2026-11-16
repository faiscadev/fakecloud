//! ECS daemons launch from their daemon task definition.
//!
//! A daemon task definition lives in its own family/revision namespace, so a
//! daemon's task must run the daemon task definition's containers (command,
//! environment, mountPoints) with its `volumes[]` mounted, never a regular
//! task definition that happens to share the family name:
//!
//! - **host volume**: a file the daemon container writes through a `host`
//!   volume shows up on the host path, with content taken from the daemon
//!   task definition's environment.
//! - **log driver volume**: a `logDriverVolumeConfiguration` volume holds the
//!   json-file logs of the other tasks' containers (one directory per
//!   container ID with `<id>-json.log`), and containers using another log
//!   driver don't write to it.
//!
//! Docker-gated the same way the other ECS runtime tests are.

mod helpers;

use std::time::Duration;

use aws_sdk_ecs::types::{
    ContainerDefinition, DaemonContainerDefinition, DaemonVolume, HostVolumeProperties,
    KeyValuePair, LogConfiguration, LogDriver, MountPoint, Task,
};
use helpers::{ecs_task_summary, TestServer};

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

fn unique() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn describe(ecs: &aws_sdk_ecs::Client, cluster: &str, arn: &str) -> Task {
    ecs.describe_tasks()
        .cluster(cluster)
        .tasks(arn)
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .clone()
}

async fn wait_status(ecs: &aws_sdk_ecs::Client, cluster: &str, arn: &str, status: &str) -> Task {
    let mut last = None;
    for _ in 0..240 {
        let task = describe(ecs, cluster, arn).await;
        if task.last_status() == Some(status) {
            return task;
        }
        if task.last_status() == Some("STOPPED") {
            panic!(
                "task {arn} stopped before {status}: {}",
                ecs_task_summary(&task)
            );
        }
        last = Some(task);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!(
        "task {arn} never reached {status}: {}",
        last.as_ref().map(ecs_task_summary).unwrap_or_default()
    );
}

/// The single task a daemon spawned (one per capacity provider).
async fn daemon_task_arn(ecs: &aws_sdk_ecs::Client, cluster: &str, daemon: &str) -> String {
    let tasks = ecs
        .list_tasks()
        .cluster(cluster)
        .started_by(format!("ecs-daemon/{daemon}"))
        .send()
        .await
        .unwrap();
    assert_eq!(tasks.task_arns().len(), 1, "{tasks:?}");
    tasks.task_arns()[0].clone()
}

async fn create_daemon(ecs: &aws_sdk_ecs::Client, cluster: &str, name: &str, td_arn: &str) {
    ecs.create_daemon()
        .daemon_name(name)
        .cluster_arn(cluster)
        .daemon_task_definition_arn(td_arn)
        .capacity_provider_arns("arn:aws:ecs:us-east-1:123456789012:capacity-provider/managed")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn ecs_daemon_task_runs_its_daemon_task_definition_with_host_volume() {
    if !require_docker_or_skip("ecs_daemon_task_runs_its_daemon_task_definition_with_host_volume") {
        return;
    }
    let server = TestServer::start().await;
    let ecs = server.ecs_client().await;
    let cluster = "daemon-host-volume";

    let host_dir = std::env::temp_dir().join(format!("fakecloud-ecs-daemon-{}", unique()));
    std::fs::create_dir_all(&host_dir).unwrap();
    let host_path = host_dir.to_string_lossy().to_string();

    ecs.create_cluster()
        .cluster_name(cluster)
        .send()
        .await
        .unwrap();

    // A regular task definition in the same family as the daemon task
    // definition: the daemon must not run it.
    ecs.register_task_definition()
        .family("agent")
        .container_definitions(
            ContainerDefinition::builder()
                .name("agent")
                .image("public.ecr.aws/docker/library/alpine:3.20")
                .command("sh")
                .command("-c")
                .command("echo regular > /mnt/data/marker.txt")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let td_arn = ecs
        .register_daemon_task_definition()
        .family("agent")
        .volumes(
            DaemonVolume::builder()
                .name("host-data")
                .host(
                    HostVolumeProperties::builder()
                        .source_path(&host_path)
                        .build(),
                )
                .build(),
        )
        .container_definitions(
            DaemonContainerDefinition::builder()
                .name("agent")
                .image("public.ecr.aws/docker/library/alpine:3.20")
                .essential(true)
                .environment(
                    KeyValuePair::builder()
                        .name("MARK")
                        .value("from-daemon-task-definition")
                        .build(),
                )
                .mount_points(
                    MountPoint::builder()
                        .source_volume("host-data")
                        .container_path("/mnt/data")
                        .build(),
                )
                .command("sh")
                .command("-c")
                .command("echo \"$MARK\" > /mnt/data/marker.txt")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .daemon_task_definition_arn()
        .unwrap()
        .to_string();

    create_daemon(&ecs, cluster, "host-agent", &td_arn).await;
    let task_arn = daemon_task_arn(&ecs, cluster, "host-agent").await;
    let task = wait_status(&ecs, cluster, &task_arn, "STOPPED").await;
    assert_eq!(
        task.containers()[0].exit_code(),
        Some(0),
        "{}",
        ecs_task_summary(&task)
    );

    let marker = std::fs::read_to_string(host_dir.join("marker.txt"))
        .expect("the daemon container wrote through its host volume");
    assert_eq!(marker.trim(), "from-daemon-task-definition");
    let _ = std::fs::remove_dir_all(&host_dir);
}

/// The SDK this suite pins predates `logDriverVolumeConfiguration`, so the
/// daemon task definition is registered over the JSON wire protocol.
async fn register_log_reader_daemon_task_definition(
    server: &TestServer,
    family: &str,
    script: &str,
) -> String {
    let body = serde_json::json!({
        "family": family,
        "volumes": [{
            "name": "task-logs",
            "logDriverVolumeConfiguration": {"driver": "json-file"}
        }],
        "containerDefinitions": [{
            "name": "reader",
            "image": "public.ecr.aws/docker/library/alpine:3.20",
            "essential": true,
            "mountPoints": [{
                "sourceVolume": "task-logs",
                "containerPath": "/logs",
                "readOnly": true
            }],
            "command": ["sh", "-c", script]
        }]
    });
    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header(
            "X-Amz-Target",
            "AmazonEC2ContainerServiceV20141113.RegisterDaemonTaskDefinition",
        )
        .header("Content-Type", "application/x-amz-json-1.1")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20260424/us-east-1/ecs/aws4_request, SignedHeaders=host, Signature=0000",
        )
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status = {}", resp.status());
    let out: serde_json::Value = resp.json().await.unwrap();
    out["daemonTaskDefinitionArn"]
        .as_str()
        .expect("daemonTaskDefinitionArn")
        .to_string()
}

#[tokio::test]
async fn ecs_daemon_log_driver_volume_holds_json_file_logs_of_other_tasks() {
    if !require_docker_or_skip("ecs_daemon_log_driver_volume_holds_json_file_logs_of_other_tasks") {
        return;
    }
    let server = TestServer::start().await;
    let ecs = server.ecs_client().await;
    let cluster = "daemon-log-volume";
    let id = unique().replace('-', "");
    let json_token = format!("jsonfile{id}");
    let other_token = format!("syslog{id}");

    ecs.create_cluster()
        .cluster_name(cluster)
        .send()
        .await
        .unwrap();

    // One container logs with json-file (its log lands in the volume), the
    // other with a different driver (its log must not).
    let workload = |name: &str, driver: LogDriver, token: &str| {
        ContainerDefinition::builder()
            .name(name)
            .image("public.ecr.aws/docker/library/alpine:3.20")
            .essential(true)
            .log_configuration(
                LogConfiguration::builder()
                    .log_driver(driver)
                    .build()
                    .unwrap(),
            )
            .command("sh")
            .command("-c")
            .command(format!("echo {token}; sleep 300"))
            .build()
    };
    ecs.register_task_definition()
        .family("logging-workload")
        .container_definitions(workload("json", LogDriver::JsonFile, &json_token))
        .container_definitions(workload("other", LogDriver::Syslog, &other_token))
        .send()
        .await
        .unwrap();
    let workload_arn = ecs
        .run_task()
        .cluster(cluster)
        .task_definition("logging-workload")
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();
    wait_status(&ecs, cluster, &workload_arn, "RUNNING").await;

    // Exit 0: the json-file container's `<id>/<id>-json.log` holds its
    // output and no json-file log holds the other container's output.
    // Exit 3: no json-file log with the token; 4: the other driver's output
    // leaked into a json-file log.
    let script = format!(
        "found=; for i in $(seq 1 60); do \
           for f in /logs/*/*-json.log; do \
             [ -f \"$f\" ] || continue; d=$(basename \"$(dirname \"$f\")\"); \
             [ \"$f\" = \"/logs/$d/$d-json.log\" ] && grep -q {json_token} \"$f\" && found=1; \
           done; \
           [ -n \"$found\" ] && break; sleep 1; \
         done; \
         [ -n \"$found\" ] || exit 3; \
         cat /logs/*/*-json.log | grep -q {other_token} && exit 4; \
         exit 0"
    );
    let td_arn = register_log_reader_daemon_task_definition(&server, "log-reader", &script).await;
    create_daemon(&ecs, cluster, "log-reader", &td_arn).await;
    let task_arn = daemon_task_arn(&ecs, cluster, "log-reader").await;
    let task = wait_status(&ecs, cluster, &task_arn, "STOPPED").await;
    let exit = task.containers()[0].exit_code();

    ecs.stop_task()
        .cluster(cluster)
        .task(&workload_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(exit, Some(0), "{}", ecs_task_summary(&task));
    let workload = describe(&ecs, cluster, &workload_arn).await;
    assert_eq!(workload.desired_status(), Some("STOPPED"));
}
