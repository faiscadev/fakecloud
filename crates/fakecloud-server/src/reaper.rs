//! Startup reaper for orphaned backing containers.
//!
//! fakecloud spawns docker containers for RDS (postgres), ElastiCache (redis),
//! Lambda (runtime images), EC2 and ECS tasks, and labels each one with
//! `fakecloud-instance=fakecloud-<server-pid>`. ECS awsvpc per-task networks
//! carry the same label. Normal shutdown runs `stop_all()` on each runtime,
//! but if the server was killed with SIGKILL (or crashed, or OOM'd) those
//! containers (and networks) outlive the process and pile up.
//!
//! On startup we list every container, network and data volume carrying the
//! `fakecloud-instance` label, parse the owning PID out of the label value,
//! and remove any whose owner is no longer alive. Objects owned by the
//! currently-running fakecloud process are always skipped.
//!
//! Memory-mode data volumes (see `fakecloud_core::data_volume`) carry the same
//! label and are reaped the same way, after the containers that mount them.
//! Durable data-dir volumes carry no ownership label, so they are never
//! touched here.

use fakecloud_core::container_net::{bounded_output, bounded_status, owned_by_dead_process};

/// Reap orphaned fakecloud-owned containers whose server PID is no longer alive.
///
/// Uses the same CLI detection policy as the runtimes: honors
/// `FAKECLOUD_CONTAINER_CLI` if set, otherwise tries `docker` then `podman`.
/// If no container CLI is available this is a silent no-op — fakecloud is
/// expected to start fine without docker.
pub fn reap_stale_containers() {
    // Uses the shared, *bounded+memoized* detection in `container_net`: an
    // unreachable/wedged daemon leaves a raw `docker info` blocked on connect
    // forever, and this reaper runs at startup, so a naive probe here would
    // hang the server (and every conformance `*_probe` test) indefinitely.
    let Some(cli) = fakecloud_core::container_net::detect_container_cli() else {
        return;
    };
    let alive = fakecloud_core::container_net::pid_alive;

    // The reap runs before the server serves its first request, and each CLI
    // call costs a daemon round trip (about 90 ms on podman machine), so the
    // three read-only listings run at once. Only the removals are ordered.
    let (containers, networks, volumes) = std::thread::scope(|s| {
        let containers = s.spawn(|| orphans(&cli, &["ps", "-a"], alive));
        let networks = s.spawn(|| orphans_by_inspect(&cli, "network", alive));
        let volumes = s.spawn(|| orphans_by_inspect(&cli, "volume", alive));
        (
            containers.join().unwrap_or_default(),
            networks.join().unwrap_or_default(),
            volumes.join().unwrap_or_default(),
        )
    });

    let reaped = remove_all(&cli, &["rm", "-f"], &containers);
    if reaped > 0 {
        tracing::info!(count = reaped, "reaped orphaned backing containers");
    }

    // ECS awsvpc per-task networks carry the same ownership label. The
    // network driver refuses removal while a container is still attached,
    // so prune networks *after* containers. `network rm` is a no-op for an
    // already-gone network, so a partial container reap above doesn't wedge
    // this pass.
    let reaped_networks = remove_all(&cli, &["network", "rm"], &networks);
    if reaped_networks > 0 {
        tracing::info!(count = reaped_networks, "reaped orphaned backing networks");
    }

    // Process-scoped data volumes outlive a killed memory-mode server just like
    // its containers. Containers go first: a volume still mounted by one can't
    // be removed.
    let reaped_volumes = remove_all(&cli, &["volume", "rm"], &volumes);
    if reaped_volumes > 0 {
        tracing::info!(count = reaped_volumes, "reaped orphaned data volumes");
    }
}

/// Networks or volumes (`kind`) labelled `fakecloud-instance=<owner>` whose
/// owner is gone. Their `ls` can't print one label portably (docker formats
/// `.Label`; podman's listing has neither `.Label` nor an indexable `.Labels`,
/// so the template fails), so the owners are read with `<kind> inspect`, whose
/// `.Labels` is a map on both engines. Networks are named by ID (older Docker
/// engines allow two networks with one name); volumes only have names.
fn orphans_by_inspect(cli: &str, kind: &str, alive: fn(u32) -> bool) -> Vec<String> {
    let key = if kind == "network" {
        "{{.ID}}"
    } else {
        "{{.Name}}"
    };
    let Some(listing) = bounded_output(
        cli,
        &[
            kind,
            "ls",
            "--filter",
            "label=fakecloud-instance",
            "--format",
            key,
        ],
    ) else {
        return Vec::new();
    };
    let ids: Vec<&str> = listing
        .lines()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .collect();
    if ids.is_empty() {
        return Vec::new();
    }
    owners(cli, kind, &ids)
        .into_iter()
        .filter(|(_, owner)| owned_by_dead_process(owner, alive))
        .map(|(id, _)| id.to_string())
        .collect()
}

/// The `fakecloud-instance` label of each object, read in one `inspect` call
/// (each call is a daemon round trip, and live servers' objects are listed
/// too). If that call fails, an object went away after the listing, so each
/// is read on its own and the missing ones are dropped.
fn owners<'a>(cli: &str, kind: &str, ids: &[&'a str]) -> Vec<(&'a str, String)> {
    const FORMAT: &str = "{{index .Labels \"fakecloud-instance\"}}";
    let mut args = vec![kind, "inspect", "--format", FORMAT];
    args.extend_from_slice(ids);
    if let Some(out) = bounded_output(cli, &args) {
        let lines: Vec<&str> = out.lines().map(str::trim).collect();
        if lines.len() == ids.len() {
            return ids
                .iter()
                .zip(lines)
                .map(|(id, owner)| (*id, owner.to_string()))
                .collect();
        }
    }
    ids.iter()
        .filter_map(|id| {
            bounded_output(cli, &[kind, "inspect", "--format", FORMAT, id])
                .map(|owner| (*id, owner.trim().to_string()))
        })
        .collect()
}

/// IDs of the objects listed by
/// `<cli> <list_args> --filter label=fakecloud-instance` whose owning PID is
/// no longer alive (the current process and live owners are skipped). Used
/// for containers, whose listing formats `.Label` on both engines.
fn orphans(cli: &str, list_args: &[&str], alive: fn(u32) -> bool) -> Vec<String> {
    let mut args: Vec<&str> = list_args.to_vec();
    args.extend_from_slice(&[
        "--filter",
        "label=fakecloud-instance",
        "--format",
        "{{.ID}} {{.Label \"fakecloud-instance\"}}",
    ]);

    // Bounded: the liveness probe answering does not promise this call will,
    // and the reap runs synchronously before the server starts serving, so an
    // unbounded call here wedges startup rather than just the sweep.
    let Some(listing) = bounded_output(cli, &args) else {
        return Vec::new();
    };
    orphan_ids(&listing, alive)
}

/// The IDs in an `<id> <owner label>` listing whose owner is a dead process.
fn orphan_ids(listing: &str, alive: fn(u32) -> bool) -> Vec<String> {
    listing
        .lines()
        .filter_map(|line| line.split_once(' '))
        .filter(|(_, label)| owned_by_dead_process(label, alive))
        .map(|(id, _)| id.to_string())
        .collect()
}

/// Run `<cli> <remove_args> <id>` for each ID, returning how many succeeded.
fn remove_all(cli: &str, remove_args: &[&str], ids: &[String]) -> usize {
    ids.iter()
        .filter(|id| {
            let mut argv = remove_args.to_vec();
            argv.push(id.as_str());
            bounded_status(cli, &argv)
        })
        .count()
}

#[cfg(all(test, unix))]
mod tests {
    use super::orphan_ids;
    use fakecloud_core::container_net::pid_alive;

    #[test]
    fn self_is_alive() {
        assert!(pid_alive(std::process::id()));
    }

    #[test]
    fn init_is_alive() {
        assert!(pid_alive(1));
    }

    #[test]
    fn huge_pid_is_dead() {
        // Max u32 is far outside any reasonable PID range on any OS.
        // Above any pid_max (Linux caps it at 2^22).
        assert!(!pid_alive(i32::MAX as u32));
    }

    #[test]
    fn orphan_ids_keeps_only_dead_owners() {
        let me = std::process::id();
        let dead = i32::MAX as u32;
        let listing = format!(
            "aaa fakecloud-{dead}\nbbb fakecloud-{me}\nccc fakecloud-1\nmalformed\nddd fakecloud-{dead}\n"
        );
        assert_eq!(orphan_ids(&listing, pid_alive), vec!["aaa", "ddd"]);
    }
}
