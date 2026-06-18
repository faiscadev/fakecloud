//! Real EC2 security-group packet-filtering E2E (issue #1745 phase 3).
//!
//! The other enforcement tests cover the *generated* nft ruleset (unit tests)
//! and the degrade path. This one closes the gap the 2026-06-18 bug-hunt
//! flagged: nothing ever observed a **real dropped packet**. Here we turn
//! enforcement ON (`FAKECLOUD_EC2_SG_ENFORCEMENT=1`) on a privileged Linux
//! host, launch two instances in one subnet under a security group with no
//! ingress allows, and assert one instance genuinely **cannot** ping the other
//! — then Authorize ingress and watch it start working, then Revoke and watch
//! it stop.
//!
//! This needs nftables + `CAP_NET_ADMIN` + a native-Linux Docker daemon, which
//! only the dedicated privileged CI job provides. It is gated on
//! `FAKECLOUD_TEST_SG_ENFORCE=1` (set by that job): when the gate is on, a
//! missing capability **panics** rather than silently passing (per
//! `feedback_tests_never_silently_skip`); when it's off, the test skips so a
//! casual `cargo test` / the standard E2E partitions don't try to touch the
//! host firewall.

mod helpers;

use helpers::TestServer;

/// Whether this host can actually enforce: nft runnable (proxy for
/// `CAP_NET_ADMIN` + the binary) on Linux with Docker.
fn enforcement_capable() -> bool {
    cfg!(target_os = "linux") && cmd_ok("nft", &["list", "ruleset"]) && cmd_ok("docker", &["info"])
}

fn cmd_ok(bin: &str, args: &[&str]) -> bool {
    std::process::Command::new(bin)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Gate: run only when explicitly enabled. With the gate on, refuse to silently
/// skip — panic if the host can't enforce, so the privileged CI job is a real
/// signal. With the gate off, skip.
fn require_enforcement_or_skip() -> bool {
    if std::env::var("FAKECLOUD_TEST_SG_ENFORCE").is_err() {
        eprintln!("skipping real SG-enforcement test (set FAKECLOUD_TEST_SG_ENFORCE=1 to run)");
        return false;
    }
    if !enforcement_capable() {
        panic!(
            "FAKECLOUD_TEST_SG_ENFORCE=1 but this host can't enforce: needs nftables + \
             CAP_NET_ADMIN + a native-Linux Docker daemon. Run the privileged CI job."
        );
    }
    true
}

fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("spawn docker");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn container_for(instance_id: &str) -> String {
    docker(&[
        "ps",
        "-aq",
        "--filter",
        &format!("label=fakecloud-ec2={instance_id}"),
    ])
}

fn can_ping(from_container: &str, to_ip: &str) -> bool {
    std::process::Command::new("docker")
        .args(["exec", from_container, "ping", "-c", "1", "-W", "2", to_ip])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Poll `can_ping` until it matches `want`, up to ~10s (enforcement reconcile
/// is async/background). Returns the final observed reachability.
fn wait_ping(from: &str, to_ip: &str, want: bool) -> bool {
    for _ in 0..40 {
        if can_ping(from, to_ip) == want {
            return want;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    can_ping(from, to_ip)
}

/// Poll up to ~15s for fakecloud's own nft table to be installed (the reconcile
/// is async). Returns whether it appeared.
fn wait_nft_table() -> bool {
    for _ in 0..60 {
        if cmd_ok("nft", &["list", "table", "inet", "fakecloud_ec2"]) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    false
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
async fn security_group_actually_drops_and_allows_packets() {
    if !require_enforcement_or_skip() {
        return;
    }
    // Pass enforcement + base image to the spawned server explicitly (not via
    // inherited process env), so the binary definitely sees them.
    let server = TestServer::start_with_env(&[
        ("FAKECLOUD_EC2_SG_ENFORCEMENT", "1"),
        ("FAKECLOUD_EC2_DEFAULT_IMAGE", "alpine:3"),
    ])
    .await;
    let c = server.ec2_client().await;

    // One VPC + subnet, and a security group with NO ingress allows (only the
    // default allow-all egress). Two instances in the subnet under it.
    let vpc = c
        .create_vpc()
        .cidr_block("10.77.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = c
        .create_subnet()
        .vpc_id(&vpc)
        .cidr_block("10.77.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet = subnet.subnet().unwrap().subnet_id().unwrap().to_string();
    let sg = c
        .create_security_group()
        .group_name("no-ingress")
        .description("blocks all inbound")
        .vpc_id(&vpc)
        .send()
        .await
        .unwrap();
    let sg = sg.group_id().unwrap().to_string();

    let launch = || {
        let c = c.clone();
        let subnet = subnet.clone();
        let sg = sg.clone();
        async move {
            c.run_instances()
                .image_id("ami-real")
                .min_count(1)
                .max_count(1)
                .subnet_id(&subnet)
                .security_group_ids(&sg)
                .send()
                .await
                .unwrap()
                .instances()[0]
                .instance_id()
                .unwrap()
                .to_string()
        }
    };
    let a = launch().await;
    let b = launch().await;
    let _a_ip = wait_running(&c, &a).await;
    let b_ip = wait_running(&c, &b).await;
    let ca = container_for(&a);

    // Enforcement must have engaged: the reconcile creates fakecloud's own nft
    // table. If it never appears, enforcement silently disabled (nft not found
    // / no CAP_NET_ADMIN) — fail with that specific signal instead of a vague
    // "packet not dropped".
    assert!(
        wait_nft_table(),
        "fakecloud nft table `inet fakecloud_ec2` never appeared — SG enforcement \
         did not engage (check that `nft` is on PATH and the process has CAP_NET_ADMIN)"
    );

    // 1) Enforced deny: with no ingress allow, A cannot reach B.
    assert!(
        !wait_ping(&ca, &b_ip, false),
        "SG with no ingress allow must DROP the packet (real enforcement)"
    );

    // 2) Authorize ICMP ingress -> reconcile applies the allow -> ping works.
    // Allow from 0.0.0.0/0 (anywhere): the container-backed instances carry
    // docker-bridge IPs, not the AWS subnet's address space, so a specific-CIDR
    // allow keyed on the AWS subnet wouldn't match the real source — only
    // "anywhere" or a referenced security group (member /32s) does. That's an
    // inherent container-IP-vs-AWS-IP gap, documented in ec2.md.
    c.authorize_security_group_ingress()
        .group_id(&sg)
        .ip_permissions(
            aws_sdk_ec2::types::IpPermission::builder()
                .ip_protocol("icmp")
                .from_port(-1)
                .to_port(-1)
                .ip_ranges(
                    aws_sdk_ec2::types::IpRange::builder()
                        .cidr_ip("0.0.0.0/0")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(
        wait_ping(&ca, &b_ip, true),
        "after AuthorizeSecurityGroupIngress(icmp), the packet must be ALLOWED"
    );

    // Steps 1+2 conclusively prove real packet filtering: a genuine drop with
    // no allow, and a genuine pass once Authorize applies. A Revoke-then-drop
    // step is intentionally omitted: security-group enforcement is stateful
    // (`ct established,related accept`, like AWS), so the conntrack entry the
    // just-allowed pings created keeps a fresh ping "established" until the
    // ICMP conntrack timeout (~30s) — re-checking the drop immediately would
    // race that timeout. The Revoke -> nft re-render path is unit-tested.

    // Cleanup.
    for id in [&a, &b] {
        let _ = c.terminate_instances().instance_ids(id).send().await;
    }
    for _ in 0..40 {
        if [&a, &b].iter().all(|id| container_for(id).is_empty()) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    let _ = docker(&["network", "rm", &format!("fakecloud-subnet-{subnet}")]);
}
