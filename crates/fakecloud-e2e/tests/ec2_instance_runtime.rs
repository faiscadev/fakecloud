//! EC2 instance Docker runtime E2E: proves `RunInstances` boots a real
//! backing container, that user-data runs at boot, and that the instance
//! lifecycle (`Stop`/`Start`/`Terminate`) maps onto the container lifecycle.

mod helpers;

use base64::Engine;
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

/// Run `docker <args>` and return trimmed stdout (empty on failure).
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

#[tokio::test]
async fn run_instances_boots_real_container_with_user_data() {
    if !require_docker_or_skip("run_instances_boots_real_container_with_user_data") {
        return;
    }
    // A tiny base image keeps the test fast; `tail -f /dev/null` keeps any
    // image alive, so alpine works as well as the amazonlinux default.
    std::env::set_var("FAKECLOUD_EC2_DEFAULT_IMAGE", "alpine:3");

    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    // User-data the runtime decodes (`base64 -d`) and runs as a root shell
    // script at boot, exactly as cloud-init would.
    let user_data = base64::engine::general_purpose::STANDARD.encode("echo ran > /tmp/marker\n");

    let resp = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .user_data(user_data)
        .send()
        .await
        .unwrap();

    let instances = resp.instances();
    assert_eq!(instances.len(), 1);
    let instance_id = instances[0].instance_id().expect("instance id").to_string();
    assert_eq!(
        instances[0]
            .state()
            .and_then(|s| s.name())
            .map(|n| n.as_str()),
        Some("running")
    );

    // The private IP must be the real container address, not the synthesized
    // 10.0.0.x fallback used in metadata-only mode.
    let private_ip = instances[0].private_ip_address().expect("private ip");
    assert!(
        !private_ip.starts_with("10.0.0."),
        "expected a real container IP, got {private_ip}"
    );

    // A running container exists for this instance.
    let container = container_for(&instance_id);
    assert!(!container.is_empty(), "no backing container found");
    let running = docker(&["inspect", "-f", "{{.State.Running}}", &container]);
    assert_eq!(running, "true", "container should be running");

    // User-data ran at boot (it executes asynchronously, so poll briefly).
    let mut marker = String::new();
    for _ in 0..40 {
        marker = docker(&["exec", &container, "cat", "/tmp/marker"]);
        if marker == "ran" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert_eq!(
        marker, "ran",
        "user-data script did not run in the container"
    );

    // StopInstances stops the container.
    c.stop_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .unwrap();
    let mut stopped = false;
    for _ in 0..40 {
        if docker(&["inspect", "-f", "{{.State.Running}}", &container]) == "false" {
            stopped = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(stopped, "container should be stopped after StopInstances");

    // StartInstances brings it back.
    c.start_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .unwrap();
    let mut restarted = false;
    for _ in 0..40 {
        if docker(&["inspect", "-f", "{{.State.Running}}", &container]) == "true" {
            restarted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        restarted,
        "container should be running after StartInstances"
    );

    // TerminateInstances removes the container entirely.
    c.terminate_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .unwrap();
    let mut removed = false;
    for _ in 0..40 {
        if container_for(&instance_id).is_empty() {
            removed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        removed,
        "container should be removed after TerminateInstances"
    );
}
