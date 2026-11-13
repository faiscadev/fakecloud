//! The startup reaper removes containers, networks and data volumes left by a
//! fakecloud process that died without cleaning up, and keeps those owned by
//! a live process.

mod helpers;

use std::time::{Duration, Instant};

use helpers::{container_cli, TestServer};

/// A PID above any pid_max, so its objects count as orphaned.
const DEAD_OWNER: &str = "fakecloud-2147483647";

fn cli(args: &[&str]) -> std::process::Output {
    std::process::Command::new(container_cli())
        .args(args)
        .output()
        .expect("container CLI must run for this test")
}

fn must(args: &[&str]) {
    let out = cli(args);
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn exists(kind: &str, name: &str) -> bool {
    cli(&[kind, "inspect", name]).status.success()
}

/// Every server on the daemon reaps the same orphans, so another test's
/// server may still be removing one when ours answers: allow it a moment.
fn eventually_gone(kind: &str, name: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while exists(kind, name) {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    true
}

/// Removes the planted objects whatever the outcome, ignoring errors so a
/// missing CLI fails the test once rather than again while unwinding.
#[derive(Default)]
struct Planted {
    containers: Vec<String>,
    networks: Vec<String>,
    volumes: Vec<String>,
}

impl Drop for Planted {
    fn drop(&mut self) {
        let run = |args: &[&str]| {
            let _ = std::process::Command::new(container_cli())
                .args(args)
                .output();
        };
        for c in &self.containers {
            run(&["rm", "-f", c]);
        }
        for n in &self.networks {
            run(&["network", "rm", n]);
        }
        for v in &self.volumes {
            run(&["volume", "rm", "-f", v]);
        }
    }
}

#[tokio::test]
async fn startup_reaps_objects_of_dead_owners_only() {
    let suffix = format!(
        "{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dead_label = format!("fakecloud-instance={DEAD_OWNER}");
    let live_label = format!("fakecloud-instance=fakecloud-{}", std::process::id());
    let dead_net = format!("fc-reaper-n-{suffix}");
    let live_net = format!("fc-reaper-live-n-{suffix}");
    let dead_vol = format!("fc-reaper-v-{suffix}");
    let live_vol = format!("fc-reaper-live-v-{suffix}");
    let dead_container = format!("fc-reaper-c-{suffix}");
    let mut planted = Planted::default();

    for (net, label) in [(&dead_net, &dead_label), (&live_net, &live_label)] {
        planted.networks.push(net.clone());
        must(&["network", "create", "--label", label, net]);
    }
    for (vol, label) in [(&dead_vol, &dead_label), (&live_vol, &live_label)] {
        planted.volumes.push(vol.clone());
        must(&["volume", "create", "--label", label, vol]);
    }
    // Attached to the network and mounting the volume, so neither can be
    // removed before the container is: the reap must keep that order.
    planted.containers.push(dead_container.clone());
    let mount = format!("{dead_vol}:/data");
    must(&[
        "create",
        "--name",
        &dead_container,
        "--label",
        &dead_label,
        "--network",
        &dead_net,
        "-v",
        &mount,
        "alpine:3.20",
        "true",
    ]);

    let server = TestServer::start().await;
    // The reap finishes before the server answers its first request.
    server
        .sqs_client()
        .await
        .list_queues()
        .send()
        .await
        .expect("first request");

    assert!(
        eventually_gone("container", &dead_container),
        "orphaned container kept"
    );
    assert!(
        eventually_gone("network", &dead_net),
        "orphaned network kept"
    );
    assert!(eventually_gone("volume", &dead_vol), "orphaned volume kept");
    assert!(exists("network", &live_net), "live owner's network removed");
    assert!(exists("volume", &live_vol), "live owner's volume removed");
}
