//! The startup reaper removes containers, networks and data volumes left by a
//! fakecloud process that died without cleaning up, and keeps those owned by
//! a live process.

mod helpers;

use helpers::{container_cli, TestServer};

/// A PID no process holds, so its objects count as orphaned.
const DEAD_OWNER: &str = "fakecloud-4294967294";

fn cli(args: &[&str]) -> std::process::Output {
    std::process::Command::new(container_cli())
        .args(args)
        .output()
        .expect("container CLI must run for this test")
}

fn exists(kind: &str, name: &str) -> bool {
    cli(&[kind, "inspect", name]).status.success()
}

/// Removes the planted objects whatever the outcome.
struct Planted {
    container: String,
    network: String,
    volumes: Vec<String>,
}

impl Drop for Planted {
    fn drop(&mut self) {
        cli(&["rm", "-f", &self.container]);
        cli(&["network", "rm", &self.network]);
        for v in &self.volumes {
            cli(&["volume", "rm", "-f", v]);
        }
    }
}

#[tokio::test]
async fn startup_reaps_objects_of_dead_owners_only() {
    let suffix = format!("{}-{}", std::process::id(), uuid_suffix());
    let live_owner = format!("fakecloud-{}", std::process::id());
    let planted = Planted {
        container: format!("fc-reaper-c-{suffix}"),
        network: format!("fc-reaper-n-{suffix}"),
        volumes: vec![
            format!("fc-reaper-v-{suffix}"),
            format!("fc-reaper-live-{suffix}"),
        ],
    };
    let dead_label = format!("fakecloud-instance={DEAD_OWNER}");
    let live_label = format!("fakecloud-instance={live_owner}");

    let created = cli(&[
        "create",
        "--name",
        &planted.container,
        "--label",
        &dead_label,
        "alpine:3.20",
        "true",
    ]);
    assert!(
        created.status.success(),
        "create container: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let net = cli(&[
        "network",
        "create",
        "--label",
        &dead_label,
        &planted.network,
    ]);
    assert!(
        net.status.success(),
        "{}",
        String::from_utf8_lossy(&net.stderr)
    );
    for (volume, label) in planted.volumes.iter().zip([&dead_label, &live_label]) {
        let vol = cli(&["volume", "create", "--label", label, volume]);
        assert!(
            vol.status.success(),
            "{}",
            String::from_utf8_lossy(&vol.stderr)
        );
    }

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
        !exists("container", &planted.container),
        "orphaned container kept"
    );
    assert!(
        !exists("network", &planted.network),
        "orphaned network kept"
    );
    assert!(
        !exists("volume", &planted.volumes[0]),
        "orphaned volume kept"
    );
    assert!(
        exists("volume", &planted.volumes[1]),
        "volume of a live owner was removed"
    );
}

fn uuid_suffix() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
