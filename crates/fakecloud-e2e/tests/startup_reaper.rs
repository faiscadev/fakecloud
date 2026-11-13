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

fn owner(kind: &str, name: &str) -> Option<String> {
    let out = cli(&[
        kind,
        "inspect",
        "--format",
        "{{index .Labels \"fakecloud-instance\"}}",
        name,
    ]);
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Plant a container owned by a dead process, attached to a network and
/// mounting a volume with the same owner, so neither can be removed before
/// the container (on Docker only the volume: a created container holds no
/// network endpoint until it starts). Until the container exists the network
/// and volume are unused orphans another test's server may reap, and a
/// reaped volume would be silently recreated without its label, so an
/// attempt counts only if both still carry it afterwards.
fn plant_orphans(planted: &mut Planted, suffix: &str) -> Option<(String, String, String)> {
    let label = format!("fakecloud-instance={DEAD_OWNER}");
    let net = format!("fc-reaper-n-{suffix}");
    let vol = format!("fc-reaper-v-{suffix}");
    let container = format!("fc-reaper-c-{suffix}");
    planted.networks.push(net.clone());
    planted.volumes.push(vol.clone());
    planted.containers.push(container.clone());
    must(&["network", "create", "--label", &label, &net]);
    must(&["volume", "create", "--label", &label, &vol]);
    let mount = format!("{vol}:/data");
    let created = cli(&[
        "create",
        "--name",
        &container,
        "--label",
        &label,
        "--network",
        &net,
        "-v",
        &mount,
        "alpine:3.20",
        "true",
    ]);
    let intact = created.status.success()
        && owner("network", &net).as_deref() == Some(DEAD_OWNER)
        && owner("volume", &vol).as_deref() == Some(DEAD_OWNER);
    intact.then_some((container, net, vol))
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
    let live_label = format!("fakecloud-instance=fakecloud-{}", std::process::id());
    let live_net = format!("fc-reaper-live-n-{suffix}");
    let live_vol = format!("fc-reaper-live-v-{suffix}");
    let mut planted = Planted::default();

    planted.networks.push(live_net.clone());
    must(&["network", "create", "--label", &live_label, &live_net]);
    planted.volumes.push(live_vol.clone());
    must(&["volume", "create", "--label", &live_label, &live_vol]);

    let (dead_container, dead_net, dead_vol) = (0..5)
        .find_map(|attempt| plant_orphans(&mut planted, &format!("{suffix}-{attempt}")))
        .expect("could not plant orphaned objects in 5 attempts");

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
