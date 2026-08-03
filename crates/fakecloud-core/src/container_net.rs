//! Shared container-to-host networking resolution for service runtimes
//! that spawn sibling containers (Lambda, ECS, RDS, ElastiCache).
//!
//! Captures the issue #1539 fix shape in one place so the four runtimes
//! that shell out to `docker`/`podman` can't drift apart again:
//!
//! - **podman** ships `host.containers.internal` as a built-in container
//!   DNS entry on every platform and must NOT receive
//!   `--add-host host.docker.internal:host-gateway` — rootless podman's
//!   gvproxy leaves the magic alias empty and the `create` fails with
//!   "host containers internal IP address is empty".
//! - **bare docker on Linux** has no `host-gateway` magic; the bridge
//!   gateway IP has to be resolved from the daemon and injected explicitly.
//! - **Docker Desktop on Mac/Windows** resolves the `host-gateway` magic
//!   value to the host's IP.
//! - when fakecloud itself runs in a container (`FAKECLOUD_IN_CONTAINER=1`,
//!   baked into the published image), the sibling containers it spawns
//!   publish their ports on the *host's* daemon — reachable from inside
//!   fakecloud's container as `host.docker.internal:<port>`, not
//!   `127.0.0.1:<port>`.

/// Actionable remediation appended to every error raised when a container
/// runtime (Docker/Podman) is required for an operation but none is
/// available. Kept in one place so RDS, Lambda, ECS, and the server startup
/// banner all surface the same fix steps and can't drift apart.
pub const CONTAINER_RUNTIME_HINT: &str = "Install and start Docker or Podman, or set FAKECLOUD_CONTAINER_CLI to your container CLI path.";

/// Auto-detect an available container CLI. Honors `FAKECLOUD_CONTAINER_CLI`
/// as an explicit override (returns `None` if the override doesn't work),
/// otherwise prefers `docker` then `podman`. Returns `None` when neither
/// is usable.
pub fn detect_container_cli() -> Option<String> {
    if let Ok(cli) = std::env::var("FAKECLOUD_CONTAINER_CLI") {
        return if cli_available(&cli) { Some(cli) } else { None };
    }
    if cli_available("docker") {
        Some("docker".to_string())
    } else if cli_available("podman") {
        Some("podman".to_string())
    } else {
        None
    }
}

/// How long to wait for `<cli> info` before giving up and treating the
/// runtime as unavailable. A healthy daemon answers in well under a second;
/// an unreachable or wedged daemon (stale `DOCKER_HOST`, Docker Desktop mid
/// start, a broken socket) can leave the CLI blocked on connect *forever*,
/// which would hang fakecloud startup and the test harness. Bounding the
/// probe turns "daemon wedged" into "no runtime detected" instead of a hang.
pub const CLI_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Process-global memo of `<cli> info` results, keyed by CLI name/path.
///
/// Container-runtime liveness is fixed for the life of a process, but every
/// service runtime (Lambda, ECS, RDS, ElastiCache, EC2, MQ, MSK, ...) probes
/// it independently at startup — a dozen-plus `detect_container_cli()` calls.
/// Without a memo each probe re-runs `docker info`; when the daemon is wedged
/// (see [`CLI_PROBE_TIMEOUT`]) those probes are serial 10s hangs that stack
/// into minutes, wedging server startup and the conformance `*_probe` tests.
/// Caching the first answer collapses that to a single probe.
static CLI_AVAILABLE_CACHE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, bool>>,
> = std::sync::OnceLock::new();

/// True when the CLI responds to `<cli> info` with success within
/// [`CLI_PROBE_TIMEOUT`] — the same liveness probe every runtime used before
/// this module existed, but bounded so an unreachable daemon can't hang the
/// caller indefinitely (the CLI blocks on connect with no timeout of its own),
/// and memoized per process so a dozen runtimes probing at startup don't each
/// pay that bound.
pub fn cli_available(cli: &str) -> bool {
    let cache =
        CLI_AVAILABLE_CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    if let Some(&cached) = cache.lock().unwrap().get(cli) {
        return cached;
    }
    let result = probe_cli(cli);
    cache.lock().unwrap().insert(cli.to_string(), result);
    result
}

/// Run the bounded `<cli> info` liveness probe once (uncached).
fn probe_cli(cli: &str) -> bool {
    let child = std::process::Command::new(cli)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    let Ok(mut child) = child else {
        return false;
    };
    let deadline = std::time::Instant::now() + CLI_PROBE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) => {}
            Err(_) => return false,
        }
        if std::time::Instant::now() >= deadline {
            // Daemon is wedged: kill the blocked probe and report unavailable.
            let _ = child.kill();
            let _ = child.wait();
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// True when `cli` is podman or a podman-compatible binary. Matches on the
/// filename component so absolute paths (`/opt/homebrew/bin/podman`) and
/// wrappers (`podman-remote`) both register as podman. Docker Desktop's
/// compatibility CLI is named `docker`, so this check is safe.
pub fn is_podman_binary(cli: &str) -> bool {
    std::path::Path::new(cli)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.contains("podman"))
        .unwrap_or(false)
}

/// Detect the Docker bridge gateway IP on Linux. Returns `None` if
/// detection fails (caller falls back to the conventional `172.17.0.1`).
pub fn detect_bridge_gateway(cli: &str) -> Option<String> {
    let output = std::process::Command::new(cli)
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{range .IPAM.Config}}{{.Gateway}}{{end}}",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let gateway = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if gateway.is_empty() || !gateway.contains('.') {
        return None;
    }
    Some(gateway)
}

/// Resolved container-to-host networking for a given CLI. Built once at
/// runtime construction and reused for every container spawn.
#[derive(Debug, Clone)]
pub struct HostNetworking {
    /// DNS name a spawned container uses to reach fakecloud on the host.
    /// `host.containers.internal` for podman, `host.docker.internal` for
    /// docker.
    pub host_alias: String,
    /// `<alias>:<value>` argument for `--add-host`, injected into every
    /// container `create`/`run`. `None` when the runtime provides the
    /// alias natively (podman).
    pub add_host_arg: Option<String>,
    /// Address fakecloud uses to reach the *sibling* containers it just
    /// spawned (readiness probes + advertised endpoints). `127.0.0.1`
    /// when fakecloud runs on the host; `host.docker.internal` when
    /// fakecloud is itself containerized (`FAKECLOUD_IN_CONTAINER=1`).
    pub sibling_host: String,
}

impl HostNetworking {
    /// Resolve networking for `cli`, reading `FAKECLOUD_IN_CONTAINER` from
    /// the process environment.
    pub fn detect(cli: &str) -> Self {
        let (host_alias, mut add_host_arg) = resolve_host_alias(cli);
        // A resolving `host.docker.internal` is only trustworthy evidence that
        // the runtime provides the alias natively (and will inject it into
        // sibling containers too) when fakecloud is itself containerized:
        // Docker-Desktop-class runtimes inject the alias into CONTAINERS, never
        // onto the host. On a bare native-Linux host a resolving alias is
        // spurious (a hijacking NXDOMAIN resolver, a stray /etc/hosts entry, or
        // a wildcard search domain), so suppressing the bridge --add-host there
        // would break the host route sibling containers need. Gate the
        // suppression on the in-container signal to avoid that regression.
        let in_container = in_container_mode(std::env::var("FAKECLOUD_IN_CONTAINER").ok());
        add_host_arg = preserve_native_host_alias(
            add_host_arg,
            in_container && host_alias_resolves(&host_alias),
        );
        let sibling_host =
            resolve_sibling_host(&host_alias, std::env::var("FAKECLOUD_IN_CONTAINER").ok());
        Self {
            host_alias,
            add_host_arg,
            sibling_host,
        }
    }

    /// Convenience: append the `--add-host <alias>:<value>` flag pair to a
    /// growing argv vector when this runtime needs an explicit mapping.
    /// No-op for podman.
    pub fn push_add_host_args(&self, argv: &mut Vec<String>) {
        if let Some(arg) = &self.add_host_arg {
            argv.push("--add-host".to_string());
            argv.push(arg.clone());
        }
    }
}

/// How long to wait for the blocking `getaddrinfo` in [`host_alias_resolves`]
/// before giving up and returning `false`. `getaddrinfo` has no timeout of its
/// own, and a slow or unreachable DNS server would otherwise block a runtime
/// thread at startup (this runs inside runtime constructors under
/// `#[tokio::main]`). Bounding it — same tradeoff as [`CLI_PROBE_TIMEOUT`] —
/// turns "DNS wedged" into "alias doesn't resolve", the safe default that keeps
/// the `--add-host` bridge mapping.
pub const HOST_ALIAS_RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// True when `host_alias` resolves via the process resolver. The `getaddrinfo`
/// call is blocking with no timeout of its own, so it runs on a spawned thread
/// bounded by [`HOST_ALIAS_RESOLVE_TIMEOUT`]; on timeout we return `false` (the
/// safe default that keeps `--add-host`). A leaked resolver thread on timeout
/// is acceptable — same tradeoff as [`probe_cli`].
fn host_alias_resolves(host_alias: &str) -> bool {
    let (tx, rx) = std::sync::mpsc::channel();
    let alias = host_alias.to_string();
    std::thread::spawn(move || {
        let resolves = std::net::ToSocketAddrs::to_socket_addrs(&(alias.as_str(), 0)).is_ok();
        let _ = tx.send(resolves);
    });
    rx.recv_timeout(HOST_ALIAS_RESOLVE_TIMEOUT).unwrap_or(false)
}

fn preserve_native_host_alias(
    add_host_arg: Option<String>,
    should_suppress: bool,
) -> Option<String> {
    if add_host_arg.is_some() && should_suppress {
        // Suppress the injected `--add-host host.docker.internal:<vm-bridge-ip>`
        // only when fakecloud is containerized AND the alias already resolves
        // (see the gate in `detect`). In that case a Docker-Desktop-class
        // runtime provides `host.docker.internal` natively inside every sibling
        // container, pointing at the real host; injecting the VM bridge-gateway
        // IP would shadow it and break the host route. On a bare host — where a
        // hijacking resolver can make the alias resolve spuriously — the caller
        // passes `false` here so native Linux docker keeps the bridge mapping
        // it genuinely needs.
        None
    } else {
        add_host_arg
    }
}

/// Compute the `(host_alias, add_host_arg)` pair for a CLI. Pure except
/// for the bridge-gateway daemon probe on Linux docker, so the macOS /
/// podman branches are unit-testable without a daemon.
pub fn resolve_host_alias(cli: &str) -> (String, Option<String>) {
    if is_podman_binary(cli) {
        // Podman provides `host.containers.internal` natively on every
        // supported platform; injecting `host-gateway` on macOS fails
        // because rootless podman's gvproxy doesn't expose the magic alias.
        ("host.containers.internal".to_string(), None)
    } else if cfg!(target_os = "linux") {
        // Bare docker on Linux: resolve the bridge gateway IP and add an
        // explicit alias. `host.docker.internal:host-gateway` only works
        // on Docker Desktop; native Linux docker has no such magic.
        let ip = detect_bridge_gateway(cli).unwrap_or_else(|| "172.17.0.1".to_string());
        (
            "host.docker.internal".to_string(),
            Some(format!("host.docker.internal:{ip}")),
        )
    } else {
        // Docker Desktop on Mac/Windows: `host-gateway` is the magic alias
        // that resolves to the host's IP.
        (
            "host.docker.internal".to_string(),
            Some("host.docker.internal:host-gateway".to_string()),
        )
    }
}

/// Decide what address fakecloud uses to reach the sibling containers it
/// just spawned. Pure helper so the env-var parsing can be tested without
/// touching the process's real environment.
///
/// - `Some("1")` / `Some("true")` (case-insensitive) -> fakecloud is in a
///   container; the siblings publish their ports on the host's daemon and
///   are reachable at the same host alias the spawned containers use to
///   reach fakecloud — `host.docker.internal` under docker,
///   `host.containers.internal` under podman. Hardcoding
///   `host.docker.internal` here broke podman, whose gvproxy network only
///   resolves `host.containers.internal` (issue #1539 follow-up).
/// - anything else, including `None` -> fakecloud runs on the host,
///   siblings live on `127.0.0.1:<port>`.
pub fn resolve_sibling_host(host_alias: &str, env_value: Option<String>) -> String {
    if in_container_mode(env_value) {
        host_alias.to_string()
    } else {
        "127.0.0.1".to_string()
    }
}

/// Parse the `FAKECLOUD_IN_CONTAINER` signal: `Some("1")` or a case-insensitive
/// `Some("true")` mean fakecloud is running inside a container; anything else,
/// including `None`, means it runs on the host. Single source of truth for the
/// parse so `detect`'s native-alias gate and `resolve_sibling_host` can't drift.
fn in_container_mode(env_value: Option<String>) -> bool {
    env_value
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Hostnames fakecloud's bundled ECR/OCI registry can be addressed by from a
/// sibling container, each at `server_port`.
///
/// A container-spawning service rewrites the image pull URI to the runtime's
/// sibling host -- `host.docker.internal` under Docker, `host.containers.internal`
/// under podman -- or leaves it `127.0.0.1` when fakecloud runs on the host. The
/// registry enforces auth, and the Docker/Podman CLI only attaches the
/// `Authorization` header for hosts present in `config.json`, so the isolated
/// pull config must list *every* alias or the pull gets a 401. The map
/// previously omitted the podman alias, so image-based Lambda/ECS pulls failed
/// under podman-in-a-container (bug-audit 2026-06-20, 0.B2). Authorize all of
/// them with the same credential; centralized here so the two builders can't
/// drift again.
pub fn registry_auth_hosts(server_port: u16) -> Vec<String> {
    [
        "localhost",
        "127.0.0.1",
        "host.docker.internal",
        "host.containers.internal",
    ]
    .iter()
    .map(|host| format!("{host}:{server_port}"))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_available_false_for_missing_binary() {
        // A binary that doesn't exist fails to spawn -> unavailable, fast.
        assert!(!cli_available("definitely-not-a-real-cli-binary-xyz-123"));
    }

    #[cfg(unix)]
    #[test]
    fn cli_available_bounds_a_hanging_probe() {
        // A CLI whose `info` invocation blocks forever (like `docker info`
        // against an unreachable daemon) must not hang the caller: the probe
        // is killed at CLI_PROBE_TIMEOUT and reported unavailable. Regression
        // test for the local-conformance-probe hang.
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("fc-clitest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let script = dir.join("hangcli");
        std::fs::write(&script, "#!/bin/sh\nsleep 600\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::io::stdout().flush().ok();

        let start = std::time::Instant::now();
        let available = cli_available(script.to_str().unwrap());
        let elapsed = start.elapsed();

        std::fs::remove_dir_all(&dir).ok();
        assert!(!available, "a hanging probe must report unavailable");
        assert!(
            elapsed < CLI_PROBE_TIMEOUT + std::time::Duration::from_secs(5),
            "probe took {elapsed:?}, expected it bounded near {CLI_PROBE_TIMEOUT:?}"
        );
    }

    #[test]
    fn is_podman_binary_matches_bare_name() {
        assert!(is_podman_binary("podman"));
        assert!(is_podman_binary("podman-remote"));
    }

    #[test]
    fn registry_auth_hosts_includes_podman_alias() {
        // The podman sibling alias (host.containers.internal) must be authorized
        // or image-based Lambda/ECS pulls 401 under podman-in-a-container (0.B2).
        let hosts = registry_auth_hosts(4566);
        assert!(hosts.contains(&"localhost:4566".to_string()));
        assert!(hosts.contains(&"127.0.0.1:4566".to_string()));
        assert!(hosts.contains(&"host.docker.internal:4566".to_string()));
        assert!(
            hosts.contains(&"host.containers.internal:4566".to_string()),
            "podman sibling alias must be authorized: {hosts:?}"
        );
    }

    #[test]
    fn is_podman_binary_matches_absolute_path() {
        assert!(is_podman_binary("/opt/homebrew/bin/podman"));
        assert!(is_podman_binary("/usr/local/bin/podman-remote"));
    }

    #[test]
    fn is_podman_binary_rejects_docker() {
        assert!(!is_podman_binary("docker"));
        assert!(!is_podman_binary("/usr/local/bin/docker"));
        assert!(!is_podman_binary("docker-credential-helper"));
    }

    #[test]
    fn resolve_host_alias_podman_has_no_add_host() {
        let (alias, add_host) = resolve_host_alias("podman");
        assert_eq!(alias, "host.containers.internal");
        assert_eq!(add_host, None);
        let (alias, add_host) = resolve_host_alias("/opt/homebrew/bin/podman");
        assert_eq!(alias, "host.containers.internal");
        assert_eq!(add_host, None);
    }

    #[test]
    fn resolve_host_alias_docker_emits_add_host() {
        let (alias, add_host) = resolve_host_alias("docker");
        assert_eq!(alias, "host.docker.internal");
        // On macOS this is host-gateway; on Linux it's a bridge IP. Either
        // way docker must get an explicit --add-host.
        assert!(add_host.is_some());
        assert!(add_host.unwrap().starts_with("host.docker.internal:"));
    }

    #[test]
    fn native_host_alias_prevents_docker_add_host_override() {
        let add_host =
            preserve_native_host_alias(Some("host.docker.internal:host-gateway".to_string()), true);

        assert_eq!(add_host, None);
    }

    #[test]
    fn unresolved_host_alias_keeps_docker_add_host() {
        let add_host = preserve_native_host_alias(
            Some("host.docker.internal:host-gateway".to_string()),
            false,
        );

        assert_eq!(
            add_host.as_deref(),
            Some("host.docker.internal:host-gateway")
        );
    }

    #[test]
    fn absent_docker_add_host_remains_absent() {
        assert_eq!(preserve_native_host_alias(None, true), None);
        assert_eq!(preserve_native_host_alias(None, false), None);
    }

    #[test]
    fn in_container_mode_parses_truthy_values() {
        assert!(in_container_mode(Some("1".to_string())));
        assert!(in_container_mode(Some("true".to_string())));
        assert!(in_container_mode(Some("True".to_string())));
        assert!(in_container_mode(Some("TRUE".to_string())));
    }

    #[test]
    fn in_container_mode_rejects_falsey_and_absent() {
        assert!(!in_container_mode(None));
        assert!(!in_container_mode(Some(String::new())));
        assert!(!in_container_mode(Some("0".to_string())));
        assert!(!in_container_mode(Some("false".to_string())));
        assert!(!in_container_mode(Some("yes".to_string())));
    }

    #[test]
    fn native_alias_gate_suppresses_only_in_container() {
        // The gate `detect` computes: `in_container && host_alias_resolves`.
        let add_host = || Some("host.docker.internal:172.17.0.1".to_string());

        // In-container + resolves -> Desktop-class runtime provides the alias
        // natively in siblings; drop the shadowing bridge mapping.
        let in_container = true;
        let resolves = true;
        assert_eq!(
            preserve_native_host_alias(add_host(), in_container && resolves),
            None,
        );

        // NOT in-container (bare host) + resolves -> the resolving alias is
        // spurious (hijacking resolver / stray hosts entry). Native Linux docker
        // needs the bridge mapping; must NOT drop it. Regression guard.
        let in_container = false;
        let resolves = true;
        assert_eq!(
            preserve_native_host_alias(add_host(), in_container && resolves).as_deref(),
            Some("host.docker.internal:172.17.0.1"),
        );

        // In-container + does NOT resolve -> nothing native to preserve; keep
        // the injected mapping.
        let in_container = true;
        let resolves = false;
        assert_eq!(
            preserve_native_host_alias(add_host(), in_container && resolves).as_deref(),
            Some("host.docker.internal:172.17.0.1"),
        );
    }

    #[test]
    fn resolve_sibling_host_defaults_to_loopback() {
        assert_eq!(
            resolve_sibling_host("host.docker.internal", None),
            "127.0.0.1"
        );
        assert_eq!(
            resolve_sibling_host("host.docker.internal", Some(String::new())),
            "127.0.0.1"
        );
        assert_eq!(
            resolve_sibling_host("host.docker.internal", Some("0".to_string())),
            "127.0.0.1"
        );
        assert_eq!(
            resolve_sibling_host("host.containers.internal", Some("false".to_string())),
            "127.0.0.1"
        );
    }

    #[test]
    fn resolve_sibling_host_uses_host_alias_when_in_container() {
        // Docker: siblings reachable at host.docker.internal.
        assert_eq!(
            resolve_sibling_host("host.docker.internal", Some("1".to_string())),
            "host.docker.internal"
        );
        assert_eq!(
            resolve_sibling_host("host.docker.internal", Some("true".to_string())),
            "host.docker.internal"
        );
        assert_eq!(
            resolve_sibling_host("host.docker.internal", Some("TRUE".to_string())),
            "host.docker.internal"
        );
        // Podman: must use host.containers.internal, NOT host.docker.internal
        // (issue #1539 follow-up — gvproxy only resolves the containers alias).
        assert_eq!(
            resolve_sibling_host("host.containers.internal", Some("1".to_string())),
            "host.containers.internal"
        );
    }

    #[test]
    fn detect_wires_sibling_host_to_podman_alias_in_container() {
        // Full path: a podman binary in a container must advertise siblings
        // at host.containers.internal. resolve_host_alias drives host_alias,
        // which resolve_sibling_host then reuses.
        let (alias, add_host) = resolve_host_alias("podman");
        assert_eq!(alias, "host.containers.internal");
        assert_eq!(add_host, None);
        assert_eq!(
            resolve_sibling_host(&alias, Some("1".to_string())),
            "host.containers.internal"
        );
    }

    #[test]
    fn push_add_host_args_noop_for_podman() {
        let net = HostNetworking {
            host_alias: "host.containers.internal".to_string(),
            add_host_arg: None,
            sibling_host: "127.0.0.1".to_string(),
        };
        let mut argv = vec!["create".to_string()];
        net.push_add_host_args(&mut argv);
        assert_eq!(argv, vec!["create".to_string()]);
    }

    #[test]
    fn push_add_host_args_emits_for_docker() {
        let net = HostNetworking {
            host_alias: "host.docker.internal".to_string(),
            add_host_arg: Some("host.docker.internal:host-gateway".to_string()),
            sibling_host: "127.0.0.1".to_string(),
        };
        let mut argv = vec!["create".to_string()];
        net.push_add_host_args(&mut argv);
        assert_eq!(
            argv,
            vec![
                "create".to_string(),
                "--add-host".to_string(),
                "host.docker.internal:host-gateway".to_string(),
            ]
        );
    }
}
