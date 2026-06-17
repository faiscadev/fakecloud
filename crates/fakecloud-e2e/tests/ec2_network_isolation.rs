//! EC2 per-subnet network isolation E2E (issue #1745 phase 2). Proves that
//! `RunInstances` attaches each backing container to a per-subnet daemon
//! network so:
//!   - instances in the *same* subnet share a bridge and can reach each other,
//!   - instances in *different* VPCs/subnets land on different bridges and
//!     cannot route to each other,
//!   - a private subnet (no internet gateway) backs onto an `--internal`
//!     network while a public/default subnet does not.
//!
//! Requires Docker (hard-fails in CI, skips locally), like the runtime E2E.

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

/// Run `docker <args>` and return trimmed stdout (empty on failure).
fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("spawn docker");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Run `docker <args>` and return whether it exited 0 (for `exec ping`).
fn docker_ok(args: &[&str]) -> bool {
    std::process::Command::new("docker")
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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

/// The set of daemon networks a container is attached to.
fn networks_of(container_id: &str) -> Vec<String> {
    docker(&[
        "inspect",
        "-f",
        "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
        container_id,
    ])
    .split_whitespace()
    .map(str::to_string)
    .collect()
}

async fn run_in_subnet(c: &aws_sdk_ec2::Client, subnet_id: Option<&str>) -> String {
    let mut req = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1);
    if let Some(s) = subnet_id {
        req = req.subnet_id(s);
    }
    let resp = req.send().await.unwrap();
    resp.instances()[0].instance_id().unwrap().to_string()
}

/// Poll DescribeInstances until `id` is `running`; return its real private IP.
async fn wait_running(c: &aws_sdk_ec2::Client, id: &str) -> String {
    for _ in 0..80 {
        let d = c
            .describe_instances()
            .instance_ids(id)
            .send()
            .await
            .unwrap();
        if let Some(inst) = d
            .reservations()
            .iter()
            .flat_map(|r| r.instances())
            .find(|i| i.instance_id() == Some(id))
        {
            if inst.state().and_then(|s| s.name()).map(|n| n.as_str()) == Some("running") {
                let ip = inst.private_ip_address().unwrap_or_default().to_string();
                if !ip.is_empty() && !ip.starts_with("10.0.0.") {
                    return ip;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("instance {id} never reached running with a real container IP");
}

#[tokio::test]
async fn instances_isolate_by_subnet_network() {
    if !require_docker_or_skip("instances_isolate_by_subnet_network") {
        return;
    }
    std::env::set_var("FAKECLOUD_EC2_DEFAULT_IMAGE", "alpine:3");

    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    // Two VPCs, each with one (private, no-IGW) subnet.
    let vpc_a = c
        .create_vpc()
        .cidr_block("10.10.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_a = vpc_a.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet_a = c
        .create_subnet()
        .vpc_id(&vpc_a)
        .cidr_block("10.10.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet_a = subnet_a.subnet().unwrap().subnet_id().unwrap().to_string();

    let vpc_b = c
        .create_vpc()
        .cidr_block("10.20.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_b = vpc_b.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet_b = c
        .create_subnet()
        .vpc_id(&vpc_b)
        .cidr_block("10.20.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet_b = subnet_b.subnet().unwrap().subnet_id().unwrap().to_string();

    // a1 + a2 in subnet A; b1 in subnet B; c1 with no subnet (default subnet).
    let a1 = run_in_subnet(&c, Some(&subnet_a)).await;
    let a2 = run_in_subnet(&c, Some(&subnet_a)).await;
    let b1 = run_in_subnet(&c, Some(&subnet_b)).await;
    let c1 = run_in_subnet(&c, None).await;

    // a1 must be up before we exec into it; its own IP isn't needed.
    wait_running(&c, &a1).await;
    let a2_ip = wait_running(&c, &a2).await;
    let b1_ip = wait_running(&c, &b1).await;
    let _c1_ip = wait_running(&c, &c1).await;

    let net_a = format!("fakecloud-subnet-{subnet_a}");
    let net_b = format!("fakecloud-subnet-{subnet_b}");
    let (ca1, ca2, cb1, cc1) = (
        container_for(&a1),
        container_for(&a2),
        container_for(&b1),
        container_for(&c1),
    );

    // Same-subnet instances share the subnet's network.
    assert!(
        networks_of(&ca1).contains(&net_a),
        "a1 should be on {net_a}, got {:?}",
        networks_of(&ca1)
    );
    assert!(
        networks_of(&ca2).contains(&net_a),
        "a2 should be on {net_a}"
    );
    // Different-VPC instance is on a different network.
    assert!(
        networks_of(&cb1).contains(&net_b),
        "b1 should be on {net_b}"
    );
    assert!(
        !networks_of(&cb1).contains(&net_a),
        "b1 must not share subnet A's network"
    );

    // Connectivity: a1 -> a2 (same subnet) works; a1 -> b1 (cross VPC) fails.
    assert!(
        docker_ok(&["exec", &ca1, "ping", "-c", "1", "-W", "2", &a2_ip]),
        "same-subnet instances should reach each other"
    );
    assert!(
        !docker_ok(&["exec", &ca1, "ping", "-c", "1", "-W", "2", &b1_ip]),
        "instances in different VPCs must not reach each other"
    );

    // The no-IGW subnet network is `--internal`; the default subnet (public,
    // has a 0.0.0.0/0 -> igw route) is not.
    assert_eq!(
        docker(&["network", "inspect", "-f", "{{.Internal}}", &net_a]),
        "true",
        "private (no-IGW) subnet should back onto an internal network"
    );
    let default_net = networks_of(&cc1)
        .into_iter()
        .find(|n| n.starts_with("fakecloud-subnet-"))
        .expect("default-subnet instance should be on a subnet network");
    assert_eq!(
        docker(&["network", "inspect", "-f", "{{.Internal}}", &default_net]),
        "false",
        "public/default subnet should not be internal"
    );

    // Cleanup: terminate instances, then remove the subnet networks.
    for id in [&a1, &a2, &b1, &c1] {
        let _ = c.terminate_instances().instance_ids(id).send().await;
    }
    for _ in 0..40 {
        if [&a1, &a2, &b1, &c1]
            .iter()
            .all(|id| container_for(id).is_empty())
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    for net in [&net_a, &net_b, &default_net] {
        let _ = docker(&["network", "rm", net]);
    }
}
