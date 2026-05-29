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

/// True when the CLI responds to `<cli> info` with success — the same
/// liveness probe every runtime used before this module existed.
pub fn cli_available(cli: &str) -> bool {
    std::process::Command::new(cli)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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
        let (host_alias, add_host_arg) = resolve_host_alias(cli);
        let sibling_host = resolve_sibling_host(std::env::var("FAKECLOUD_IN_CONTAINER").ok());
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
///   container, siblings live on `host.docker.internal:<port>`.
/// - anything else, including `None` -> fakecloud runs on the host,
///   siblings live on `127.0.0.1:<port>`.
pub fn resolve_sibling_host(env_value: Option<String>) -> String {
    let in_container = env_value
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if in_container {
        "host.docker.internal".to_string()
    } else {
        "127.0.0.1".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_podman_binary_matches_bare_name() {
        assert!(is_podman_binary("podman"));
        assert!(is_podman_binary("podman-remote"));
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
    fn resolve_sibling_host_defaults_to_loopback() {
        assert_eq!(resolve_sibling_host(None), "127.0.0.1");
        assert_eq!(resolve_sibling_host(Some(String::new())), "127.0.0.1");
        assert_eq!(resolve_sibling_host(Some("0".to_string())), "127.0.0.1");
        assert_eq!(resolve_sibling_host(Some("false".to_string())), "127.0.0.1");
    }

    #[test]
    fn resolve_sibling_host_uses_docker_internal_when_in_container() {
        assert_eq!(
            resolve_sibling_host(Some("1".to_string())),
            "host.docker.internal"
        );
        assert_eq!(
            resolve_sibling_host(Some("true".to_string())),
            "host.docker.internal"
        );
        assert_eq!(
            resolve_sibling_host(Some("TRUE".to_string())),
            "host.docker.internal"
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
