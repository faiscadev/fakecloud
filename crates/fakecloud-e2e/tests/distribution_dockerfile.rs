//! Cheap, PR-gating guards on the shipped Docker image's contents.
//!
//! The real `docker run --entrypoint nft` artifact smoke lives in the Docker
//! workflow, but that only runs post-merge. This test catches a regression on
//! the PR itself — it's exactly the missing-`nft` class (bug-hunt 2026-06-18
//! finding 0.1 / #1539 Bug 4) where a documented feature can't work in the
//! published artifact because its binary was never installed.

use std::path::PathBuf;

fn dockerfile() -> String {
    // The e2e crate lives at crates/fakecloud-e2e; the Dockerfile is at the
    // repo root, two levels up.
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "..", "..", "Dockerfile"]
        .iter()
        .collect();
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn image_installs_nftables_for_sg_enforcement() {
    let df = dockerfile();
    assert!(
        df.contains("nftables"),
        "Dockerfile must `apt-get install nftables` so EC2 security-group \
         enforcement (FAKECLOUD_EC2_SG_ENFORCEMENT) can actually apply nft \
         rules in the published image — without it the feature silently \
         degrades even with CAP_NET_ADMIN"
    );
}

#[test]
fn image_installs_docker_cli_for_container_backends() {
    // Regression guard for the other shelled-to binary the runtime needs
    // (#1539 Bug 4): the docker CLI must be copied into the final image.
    let df = dockerfile();
    assert!(
        df.contains("/usr/local/bin/docker"),
        "Dockerfile must provide the docker CLI for container-backed services"
    );
}
