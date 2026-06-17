//! EC2 security-group enforcement degrade path (issue #1745 phase 3).
//!
//! Real packet filtering needs nftables + `CAP_NET_ADMIN`, which CI doesn't
//! have. The *enforcement* ruleset is unit-tested in
//! `fakecloud_ec2::runtime::firewall`; this E2E proves the other half: when
//! enforcement is requested but the host can't back it, fakecloud **degrades
//! gracefully** — instances still boot and phase-2 L3 isolation still holds,
//! with no new blocking (no regression).
//!
//! Requires Docker (hard-fails in CI, skips locally).

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

/// Whether the host can actually back nftables enforcement. When false, we
/// expect the degrade path; when true (a privileged runner), enforcement is
/// active and the deny-by-default SG would block the ping, so we don't assert
/// reachability.
fn nft_enforceable() -> bool {
    std::process::Command::new("nft")
        .args(["list", "ruleset"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("spawn docker");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn docker_ok(args: &[&str]) -> bool {
    std::process::Command::new("docker")
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn container_for(instance_id: &str) -> String {
    docker(&[
        "ps",
        "-aq",
        "--filter",
        &format!("label=fakecloud-ec2={instance_id}"),
    ])
}

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
async fn sg_enforcement_degrades_without_net_admin() {
    if !require_docker_or_skip("sg_enforcement_degrades_without_net_admin") {
        return;
    }
    // Request enforcement; on an unprivileged host this degrades to
    // metadata-only and must not regress phase-2 behavior.
    std::env::set_var("FAKECLOUD_EC2_SG_ENFORCEMENT", "1");
    std::env::set_var("FAKECLOUD_EC2_DEFAULT_IMAGE", "alpine:3");

    let server = TestServer::start().await;
    let c = server.ec2_client().await;

    let vpc = c
        .create_vpc()
        .cidr_block("10.40.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = c
        .create_subnet()
        .vpc_id(&vpc)
        .cidr_block("10.40.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet = subnet.subnet().unwrap().subnet_id().unwrap().to_string();

    // A security group with NO ingress allows (deny-by-default). Under real
    // enforcement this would block inbound; under degrade it blocks nothing.
    let sg = c
        .create_security_group()
        .group_name("locked-down")
        .description("no ingress")
        .vpc_id(&vpc)
        .send()
        .await
        .unwrap();
    let sg = sg.group_id().unwrap().to_string();

    let mut launch = |n: &str| {
        let c = c.clone();
        let subnet = subnet.clone();
        let sg = sg.clone();
        let n = n.to_string();
        async move {
            let r = c
                .run_instances()
                .image_id("ami-12345678")
                .min_count(1)
                .max_count(1)
                .subnet_id(&subnet)
                .security_group_ids(&sg)
                .send()
                .await
                .unwrap();
            let _ = n;
            r.instances()[0].instance_id().unwrap().to_string()
        }
    };
    let a = launch("a").await;
    let b = launch("b").await;

    let _a_ip = wait_running(&c, &a).await;
    let b_ip = wait_running(&c, &b).await;

    // Both booted (graceful degrade never blocks the boot).
    let ca = container_for(&a);
    let cb = container_for(&b);
    assert!(
        !ca.is_empty() && !cb.is_empty(),
        "both instances should boot"
    );

    if !nft_enforceable() {
        // Degrade path: the deny-by-default SG is NOT enforced, so same-subnet
        // reachability is unchanged from phase 2.
        assert!(
            docker_ok(&["exec", &ca, "ping", "-c", "1", "-W", "2", &b_ip]),
            "with enforcement degraded, same-subnet instances must still reach each other"
        );
    } else {
        eprintln!("nft enforceable on this host; skipping degrade reachability assertion");
    }

    for id in [&a, &b] {
        let _ = c.terminate_instances().instance_ids(id).send().await;
    }
    for _ in 0..40 {
        if [&a, &b].iter().all(|id| container_for(id).is_empty()) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    let _ = docker(&["network", "rm", &format!("fakecloud-subnet-{subnet}")]);
}
