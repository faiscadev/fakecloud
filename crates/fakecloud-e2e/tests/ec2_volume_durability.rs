//! EC2 instance data-plane durability E2E: with `FAKECLOUD_PERSIST_EC2_VOLUMES`
//! enabled, a file an instance writes under the durable data directory
//! survives a fakecloud restart -- the recovery path recreates the backing
//! container and reattaches the same named volume, so the bytes come back
//! (EBS-root-volume-style persistence for the instance's persistent data dir).
//! Distinct from `ec2_persistence.rs`, which covers control-plane metadata
//! only with no container runtime.

mod helpers;

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

fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("spawn docker");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The container id backing an instance, via the `fakecloud-ec2=<id>` label.
fn container_for(instance_id: &str) -> String {
    docker(&[
        "ps",
        "-aq",
        "--filter",
        &format!("label=fakecloud-ec2={instance_id}"),
    ])
}

/// Poll DescribeInstances until the instance is `running` and backed by a real
/// container, returning that container id. Panics if it never comes up.
async fn wait_running_container(c: &aws_sdk_ec2::Client, instance_id: &str) -> String {
    for _ in 0..80 {
        let d = c
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .unwrap();
        let running = d
            .reservations()
            .iter()
            .flat_map(|r| r.instances())
            .find(|i| i.instance_id() == Some(instance_id))
            .and_then(|i| i.state())
            .and_then(|s| s.name())
            .map(|n| n.as_str())
            == Some("running");
        if running {
            let container = container_for(instance_id);
            if !container.is_empty() {
                return container;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("instance {instance_id} never reached running with a backing container");
}

#[tokio::test]
async fn instance_data_survives_fakecloud_restart() {
    let test = "instance_data_survives_fakecloud_restart";
    if !require_docker_or_skip(test) {
        return;
    }
    // Tiny base image keeps the test fast; `tail -f /dev/null` keeps it alive.
    std::env::set_var("FAKECLOUD_EC2_DEFAULT_IMAGE", "alpine:3");
    // Turn on durable instance volumes for this run.
    std::env::set_var("FAKECLOUD_PERSIST_EC2_VOLUMES", "1");

    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let c = server.ec2_client().await;

    let instance_id = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .send()
        .await
        .unwrap()
        .instances
        .unwrap()
        .first()
        .unwrap()
        .instance_id
        .clone()
        .unwrap();

    let container = wait_running_container(&c, &instance_id).await;

    // Write a marker into the durable data dir (matches the runtime default
    // FAKECLOUD_EC2_INSTANCE_DATA_DIR=/var/lib/fakecloud/ec2). The named volume
    // is mounted there, so this byte should outlive the container.
    let write = docker(&[
        "exec",
        &container,
        "sh",
        "-c",
        "mkdir -p /var/lib/fakecloud/ec2 && echo persisted-marker > /var/lib/fakecloud/ec2/marker && cat /var/lib/fakecloud/ec2/marker",
    ]);
    assert_eq!(write, "persisted-marker", "marker should be written");

    // Restart fakecloud against the same data path. The EC2 recovery path
    // recreates the backing container, reattaching the same named volume.
    server.restart().await;
    let c = server.ec2_client().await;

    let recovered = wait_running_container(&c, &instance_id).await;
    // A fresh container backs the recovered instance...
    assert_ne!(
        recovered, container,
        "restart should recreate the backing container"
    );

    // ...but the durable volume re-attaches, so the marker is still there.
    let mut marker = String::new();
    for _ in 0..40 {
        marker = docker(&["exec", &recovered, "cat", "/var/lib/fakecloud/ec2/marker"]);
        if marker == "persisted-marker" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert_eq!(
        marker, "persisted-marker",
        "instance data dir should survive a fakecloud restart via the durable volume"
    );

    // TerminateInstances drops the container and its durable volume.
    c.terminate_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .unwrap();
    for _ in 0..40 {
        if container_for(&instance_id).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}
