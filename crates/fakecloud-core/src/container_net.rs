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
    let child = spawn_bounded(
        std::process::Command::new(cli)
            .arg("info")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
    );
    let Ok(mut child) = child else {
        return false;
    };
    wait_bounded_group(&mut child) && child.wait().map(|s| s.success()).unwrap_or(false)
}

/// Spawn a container-CLI command in a process group of its own (Unix), so a
/// timed-out call can be torn down whole. `FAKECLOUD_CONTAINER_CLI` is
/// routinely a wrapper -- `sh -c 'exec docker "$@"'`, a `podman-remote` shim --
/// which makes the real command a *grandchild*: it survives `Child::kill`, goes
/// on holding whatever pipes we handed it, and keeps running against a wedged
/// daemon forever. Its own group makes it reachable by a single signal.
/// Detaching these from terminal job control is fine: their lifetime is managed
/// by deadline here, not by the shell fakecloud was started from.
///
/// stdin is /dev/null, and has to be: a new process group is a *background*
/// one, so a child that reads the controlling terminal -- a `sudo` or
/// credential-helper wrapper prompting, exactly the wrapper case above -- takes
/// SIGTTIN, which stops it rather than ending it. `try_wait` is WNOHANG without
/// WUNTRACED, so the loop below never sees a stopped child and the call burns
/// the whole deadline before being killed. These calls are non-interactive
/// anyway, so an immediate EOF is the right answer for them.
fn spawn_bounded(cmd: &mut std::process::Command) -> std::io::Result<std::process::Child> {
    cmd.stdin(std::process::Stdio::null());
    #[cfg(unix)]
    {
        std::os::unix::process::CommandExt::process_group(cmd, 0);
    }
    cmd.spawn()
}

/// Wait for `child` up to [`CLI_PROBE_TIMEOUT`], killing it on expiry. Returns
/// whether it exited on its own. Every container-CLI call goes through this:
/// a liveness probe answering does not promise the next call will, and an
/// unbounded one blocks the caller rather than just that command.
///
/// Only for a child from [`spawn_bounded`], which put it in a group of its own:
/// the expiry kill hits that whole group, so a wrapper CLI's grandchildren die
/// with it.
fn wait_bounded_group(child: &mut std::process::Child) -> bool {
    let deadline = std::time::Instant::now() + CLI_PROBE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        if std::time::Instant::now() >= deadline {
            // Daemon is wedged: kill the blocked call and report failure.
            kill_expired(child);
            let _ = child.wait();
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// SIGKILL a timed-out child and its process group. [`spawn_bounded`] made the
/// child its own group leader, so the group id is the child's pid, and the
/// child is still unreaped here -- the pid cannot have been recycled and the
/// signal cannot stray onto an unrelated group.
#[cfg(unix)]
fn kill_expired(child: &mut std::process::Child) {
    // SAFETY: `kill` with a negative pid targets the process group of that
    // id; any pid value is safe to pass.
    let _ = unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL) };
    let _ = child.kill();
}

/// Windows has no process-group signal (a job object would be needed), so the
/// direct child is as far as the kill reaches; [`run_bounded`] still bounds the
/// wait on its stdout reader so the caller can't be held by a surviving
/// grandchild.
#[cfg(not(unix))]
fn kill_expired(child: &mut std::process::Child) {
    let _ = child.kill();
}

/// Whether the stdout reader thread ended before the call returned.
#[derive(Debug)]
enum ReaderState {
    /// The reader returned; its thread is gone.
    Finished,
    /// The reader is still blocked on the pipe because a write end we could not
    /// close is held outside the child's process group. The thread outlives the
    /// call; the caller does not wait for it.
    Abandoned,
}

/// Floor on how long [`run_bounded`] waits for its stdout reader once the call
/// is over (it also gets whatever is left of the call's own budget). Both exits
/// close every write end we control -- the child exited, or its whole process
/// group was killed -- which ends the blocked `read_to_end` at once, so this
/// covers scheduling only. It exists so a write end held somewhere we cannot
/// reach costs the caller a few hundred milliseconds instead of blocking it for
/// good, which is what an unbounded join did.
const READER_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

/// Run a container-CLI command and return its stdout, or `None` when it fails
/// or outruns [`CLI_PROBE_TIMEOUT`].
pub fn bounded_output(cli: &str, args: &[&str]) -> Option<String> {
    run_bounded(cli, args).0
}

/// [`bounded_output`], plus whether its stdout reader finished -- so the
/// timeout path's "no reader left behind" guarantee is unit-testable instead of
/// only observable as a thread that never goes away.
fn run_bounded(cli: &str, args: &[&str]) -> (Option<String>, ReaderState) {
    let deadline = std::time::Instant::now() + CLI_PROBE_TIMEOUT;
    let child = spawn_bounded(
        std::process::Command::new(cli)
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null()),
    );
    let Ok(mut child) = child else {
        return (None, ReaderState::Finished);
    };
    let Some(mut stdout) = child.stdout.take() else {
        kill_expired(&mut child);
        let _ = child.wait();
        return (None, ReaderState::Finished);
    };
    // Drain stdout while waiting. A child whose output outgrows the pipe
    // buffer blocks on write until someone reads it, so waiting for exit
    // first would deadlock until the deadline and then report the sweep as
    // failed -- `docker ps -a` across a busy host is exactly that much output.
    //
    // The channel doubles as the reader's "I'm done" signal: the send is the
    // last thing the thread does before dropping the pipe's read end, so a
    // received buffer proves no reader is parked behind us. A `JoinHandle`
    // can't say that without blocking, which on the timeout path is exactly
    // what we must not do.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stdout, &mut buf);
        let _ = tx.send(buf);
    });
    // On expiry `wait_bounded_group` has killed the whole process group, so a
    // wrapper CLI's grandchild releases the write end and the reader returns
    // instead of blocking for the life of the process -- one leaked thread per
    // call, on precisely the wedged-daemon path these bounds exist for.
    let exited = wait_bounded_group(&mut child);
    let status = child.wait().ok();
    // Whatever is left of the call's own budget, and never less than the grace:
    // a prompt call can afford to wait out a reader thread the scheduler hasn't
    // run yet, a timed-out one gets only the grace, and either way the caller is
    // back within CLI_PROBE_TIMEOUT plus that grace.
    let grace = deadline
        .saturating_duration_since(std::time::Instant::now())
        .max(READER_DRAIN_GRACE);
    let drained = rx.recv_timeout(grace).ok();
    let output = match (exited, status, &drained) {
        (true, Some(status), Some(buf)) if status.success() => {
            Some(String::from_utf8_lossy(buf).into_owned())
        }
        _ => None,
    };
    let reader = if drained.is_some() {
        ReaderState::Finished
    } else {
        ReaderState::Abandoned
    };
    (output, reader)
}

/// Run a container-CLI command for its effect only, bounded the same way.
/// Returns whether it succeeded.
pub fn bounded_status(cli: &str, args: &[&str]) -> bool {
    let Ok(mut child) = spawn_bounded(
        std::process::Command::new(cli)
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
    ) else {
        return false;
    };
    wait_bounded_group(&mut child) && child.wait().map(|s| s.success()).unwrap_or(false)
}

/// True if the given PID is a live process on this host.
///
/// On Unix this is `kill(pid, 0)`: it returns 0 if the process exists
/// (including zombies), or sets `errno` to `ESRCH` if not. On non-Unix
/// platforms it conservatively returns `true`, so a caller never removes a
/// resource it can't prove is orphaned.
#[cfg(unix)]
pub fn pid_alive(pid: u32) -> bool {
    // SAFETY: `kill` with signal 0 is a liveness probe; it does not
    // actually deliver a signal. Any PID value is safe to pass.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    // errno == EPERM means the process exists but we can't signal it —
    // still alive from our perspective.
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
pub fn pid_alive(_pid: u32) -> bool {
    true
}

/// Whether a container or network labelled `fakecloud-instance=<label>` was
/// left behind by a fakecloud process that is gone. The label is
/// `fakecloud-<pid>`; an object is orphaned only when that PID is neither the
/// current process nor alive. Several fakecloud processes can share one
/// daemon (parallel test servers, side-by-side installs), so an object owned
/// by *another live* process is never an orphan. A label that doesn't parse is
/// not treated as an orphan either -- nothing proves its owner is gone.
pub fn owned_by_dead_process(label: &str, is_alive: impl Fn(u32) -> bool) -> bool {
    let Some(pid) = label
        .strip_prefix("fakecloud-")
        .and_then(|p| p.parse::<u32>().ok())
    else {
        return false;
    };
    pid != std::process::id() && !is_alive(pid)
}

/// Which container engine a CLI actually drives. Decides the podman-only
/// code paths: `host.containers.internal` without `--add-host`, and
/// `--tls-verify=false` when pulling from fakecloud's plain-HTTP registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerEngine {
    Docker,
    Podman,
}

/// Process-global memo of [`is_podman`] results, keyed by CLI name/path. The
/// engine behind a CLI is fixed for the life of the process, and the probe
/// runs a subprocess, so every runtime constructor and image pull after the
/// first reads the answer from here.
static PODMAN_CACHE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, bool>>,
> = std::sync::OnceLock::new();

fn podman_cache() -> &'static std::sync::Mutex<std::collections::HashMap<String, bool>> {
    PODMAN_CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// True when `cli` drives podman -- including podman installed *as* `docker`.
///
/// The file name alone can't answer that: the `podman-docker` package
/// (Fedora, RHEL, CentOS Stream, ...) ships a `docker` shim that execs podman,
/// and a user may point `FAKECLOUD_CONTAINER_CLI` at any wrapper. So:
///
/// 1. fast path: a name containing `podman` (`podman`, `podman-remote`,
///    `/opt/homebrew/bin/podman`) is podman without running anything;
/// 2. otherwise ask the CLI: `<cli> --version` prints `podman version X.Y.Z`
///    through the shim and `Docker version X.Y.Z, build ...` for Docker;
/// 3. when that answers with something neither (a custom wrapper), look for
///    podman-only fields in `<cli> info`.
///
/// Every call is bounded by [`CLI_PROBE_TIMEOUT`] (a wedged CLI costs one
/// bound, then reads as Docker, the historical default) and the answer is
/// memoized per CLI. Blocking: from async code use [`is_podman_async`].
pub fn is_podman(cli: &str) -> bool {
    if name_indicates_podman(cli) {
        return true;
    }
    if let Some(&cached) = podman_cache().lock().unwrap().get(cli) {
        return cached;
    }
    let podman = probe_engine(cli) == Some(ContainerEngine::Podman);
    podman_cache()
        .lock()
        .unwrap()
        .insert(cli.to_string(), podman);
    podman
}

/// [`is_podman`] for async callers: a cached (or name-matched) answer returns
/// at once, and a first-time probe runs on the blocking pool so it can't stall
/// a runtime worker for up to [`CLI_PROBE_TIMEOUT`].
pub async fn is_podman_async(cli: &str) -> bool {
    if name_indicates_podman(cli) {
        return true;
    }
    if let Some(&cached) = podman_cache().lock().unwrap().get(cli) {
        return cached;
    }
    let owned = cli.to_string();
    tokio::task::spawn_blocking(move || is_podman(&owned))
        .await
        .unwrap_or(false)
}

/// The name-only fast path: the file name component contains `podman`, so
/// absolute paths and `podman-remote` register. Docker's CLI and the
/// `podman-docker` shim are both named `docker` and fall through to the probe.
fn name_indicates_podman(cli: &str) -> bool {
    std::path::Path::new(cli)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.contains("podman"))
        .unwrap_or(false)
}

/// Ask the CLI what it is (uncached). `None` when it can't tell -- the CLI
/// failed, timed out, or answered with neither engine's markers.
fn probe_engine(cli: &str) -> Option<ContainerEngine> {
    // A failed or timed-out `--version` means the CLI isn't answering at
    // all; don't spend a second bound on `info` against it.
    let version = bounded_output(cli, &["--version"])?;
    classify_version_output(&version).or_else(|| {
        bounded_output(cli, &["info", "--format", "{{json .}}"])
            .and_then(|info| classify_info_output(&info))
    })
}

/// Classify `<cli> --version` output. Podman prints `podman version 5.2.0`
/// (also through the `podman-docker` shim), Docker `Docker version 27.3.1,
/// build ce12230`. Podman is checked first: the shim is podman whatever else
/// the output mentions.
pub fn classify_version_output(stdout: &str) -> Option<ContainerEngine> {
    let lower = stdout.to_ascii_lowercase();
    if lower.contains("podman") {
        Some(ContainerEngine::Podman)
    } else if lower.contains("docker") {
        Some(ContainerEngine::Docker)
    } else {
        None
    }
}

/// Classify `<cli> info --format '{{json .}}'` output by engine-specific
/// fields: podman's host section carries `buildahVersion` / `ociRuntime`
/// (camelCase), Docker's top level `ServerVersion`.
pub fn classify_info_output(stdout: &str) -> Option<ContainerEngine> {
    if stdout.contains("\"buildahVersion\"") || stdout.contains("\"ociRuntime\"") {
        Some(ContainerEngine::Podman)
    } else if stdout.contains("\"ServerVersion\"") {
        Some(ContainerEngine::Docker)
    } else {
        None
    }
}

/// Detect the Docker bridge gateway IP on Linux. Returns `None` if
/// detection fails (caller falls back to the conventional `172.17.0.1`).
///
/// Goes through [`bounded_output`] like every other container-CLI call here:
/// `network inspect` talks to the same daemon as the liveness probe, so a
/// wedged one blocks it on connect forever. This runs inside runtime
/// constructors on Linux, where an unbounded call hangs server startup outright
/// -- the exact failure [`CLI_PROBE_TIMEOUT`] exists to prevent. On timeout the
/// caller just takes the conventional fallback.
pub fn detect_bridge_gateway(cli: &str) -> Option<String> {
    let stdout = bounded_output(
        cli,
        &[
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{range .IPAM.Config}}{{.Gateway}}{{end}}",
        ],
    )?;
    let gateway = stdout.trim().to_string();
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
    if is_podman(cli) {
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

/// Hostnames fakecloud's bundled ECR/OCI registry can be addressed from a
/// sibling container or the host, each at `server_port`.
///
/// A container-spawning service rewrites the image pull URI to the runtime's
/// sibling host -- `host.docker.internal` under Docker, `host.containers.internal`
/// under podman -- or leaves it `localhost` / `127.0.0.1` when fakecloud runs on
/// the host (`localhost:<port>` is the documented local ECR endpoint, e.g.
/// `localhost:4566`). The registry enforces auth, and the Docker/Podman CLI only
/// attaches the `Authorization` header for hosts present in `config.json`, so the
/// isolated pull config must list *every* alias or the pull gets a 401. The map
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

/// Host the container engine pulls fakecloud-ECR images from.
///
/// The pull is performed by the engine on the *host*, not by a sibling
/// container, so the sibling host alias is the wrong address for it: Docker
/// Desktop and OrbStack only accept a plain-HTTP registry over loopback, and
/// `host.docker.internal` does not resolve on a Linux host at all. Defaults to
/// `127.0.0.1`. Podman on macOS / Windows pulls inside its machine VM, where
/// loopback is the VM and only `host.containers.internal` reaches the host.
/// `FAKECLOUD_ECR_REGISTRY_HOST` overrides both (e.g. when a containerized
/// fakecloud's port is published under another address).
pub fn ecr_registry_host(cli: &str) -> String {
    resolve_ecr_registry_host(
        std::env::var("FAKECLOUD_ECR_REGISTRY_HOST").ok(),
        is_podman(cli),
        cfg!(target_os = "linux"),
    )
}

/// Pure half of [`ecr_registry_host`].
pub fn resolve_ecr_registry_host(env_value: Option<String>, podman: bool, linux: bool) -> String {
    if let Some(v) = env_value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        return v;
    }
    if podman && !linux {
        "host.containers.internal".to_string()
    } else {
        "127.0.0.1".to_string()
    }
}

/// Host port from `<cli> port <container> <port>` output. Docker prints one
/// `<ip>:<port>` line per bound address family (`0.0.0.0:49153`,
/// `[::]:49153`); podman prints the same shape. The first parseable port wins.
pub fn parse_published_port(output: &str) -> Option<u16> {
    output
        .lines()
        .filter_map(|l| l.trim().rsplit(':').next())
        .find_map(|p| p.parse::<u16>().ok())
}

const LOOPBACK_NAMES: [&str; 2] = ["127.0.0.1", "localhost"];

/// Rewrite loopback references in an environment value a sibling container
/// will read, so they reach the host instead of the container itself.
///
/// Inside a container `127.0.0.1` / `localhost` is the container, so every
/// endpoint fakecloud hands out on loopback -- the server URL, an RDS or
/// ElastiCache endpoint address, an MSK bootstrap string -- has to name
/// `target_host` (the host alias) instead. Rewritten:
///
/// - the whole value being a loopback host (`DB_HOST=127.0.0.1`);
/// - a loopback host followed by `:<port>`, at a token boundary -- in URLs
///   with or without userinfo (`postgres://u:p@localhost:5432/db`), bare
///   `host:port` values and comma-separated lists
///   (`127.0.0.1:9092,127.0.0.1:9094`);
/// - a URL host without a port (`http://localhost/path`).
///
/// A port a service registered with
/// [`crate::dataplane::register_container_port`] is swapped for its
/// container-view port. Prose such as `EHLO localhost`, and names that merely
/// contain a loopback name (`127.0.0.10`, `localhost.localdomain`), are left
/// alone. When `target_host` is itself loopback (fakecloud and the workload
/// share a network namespace) the value is returned unchanged.
pub fn rewrite_loopback_value(value: &str, target_host: &str) -> String {
    if LOOPBACK_NAMES.contains(&target_host) {
        return value.to_string();
    }
    if LOOPBACK_NAMES.contains(&value.trim()) {
        return value.replacen(value.trim(), target_host, 1);
    }
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        let hit = LOOPBACK_NAMES
            .iter()
            .find(|name| value[i..].starts_with(**name) && loopback_at(value, i, name.len()));
        let Some(name) = hit else {
            let ch = value[i..].chars().next().unwrap_or_default();
            out.push(ch);
            i += ch.len_utf8().max(1);
            continue;
        };
        out.push_str(target_host);
        i += name.len();
        // Swap a registered host port for its container-view port.
        if bytes.get(i) == Some(&b':') {
            let digits: String = value[i + 1..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Some(mapped) = digits
                .parse::<u16>()
                .ok()
                .and_then(crate::dataplane::container_port_for)
            {
                out.push(':');
                out.push_str(&mapped.to_string());
                i += 1 + digits.len();
            }
        }
    }
    out
}

/// Whether the loopback name at `value[i..i + len]` is a host reference: at a
/// token boundary and followed by `:<digit>`, or a URL host (after `//` or
/// `@`) followed by the end of the authority.
fn loopback_at(value: &str, i: usize, len: usize) -> bool {
    let before = value[..i].chars().next_back();
    let boundary_before = match before {
        None => true,
        Some(c) => matches!(c, '/' | '@' | ',' | '=' | ';' | '(' | '"' | '\'') || c.is_whitespace(),
    };
    if !boundary_before {
        return false;
    }
    let rest = &value[i + len..];
    let mut after = rest.chars();
    match after.next() {
        Some(':') => after.next().is_some_and(|c| c.is_ascii_digit()),
        next => {
            let url_host = matches!(before, Some('@')) || value[..i].ends_with("//");
            url_host && matches!(next, None | Some('/') | Some('?') | Some('#'))
        }
    }
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
    fn name_fast_path_matches_podman_names() {
        assert!(name_indicates_podman("podman"));
        assert!(name_indicates_podman("podman-remote"));
        assert!(name_indicates_podman("/opt/homebrew/bin/podman"));
        assert!(name_indicates_podman("/usr/local/bin/podman-remote"));
        assert!(!name_indicates_podman("docker"));
        assert!(!name_indicates_podman("/usr/local/bin/docker"));
        assert!(!name_indicates_podman("docker-credential-helper"));
    }

    #[test]
    fn name_fast_path_needs_no_probe() {
        // No binary by this name exists: a podman-named CLI is podman without
        // running anything.
        assert!(is_podman("/nonexistent-dir-fc-2599/podman"));
        assert!(is_podman("podman-remote-definitely-missing-xyz"));
    }

    #[test]
    fn version_output_classifies_the_engine() {
        assert_eq!(
            classify_version_output("podman version 5.2.0\n"),
            Some(ContainerEngine::Podman)
        );
        assert_eq!(
            classify_version_output("Docker version 27.3.1, build ce12230\n"),
            Some(ContainerEngine::Docker)
        );
        // The podman-docker shim's notice mentions Docker; it is still podman.
        assert_eq!(
            classify_version_output(
                "Emulate Docker CLI using podman. Create /etc/containers/nodocker to quiet msg.\npodman version 4.9.4\n"
            ),
            Some(ContainerEngine::Podman)
        );
        assert_eq!(classify_version_output("my-wrapper 1.0\n"), None);
        assert_eq!(classify_version_output(""), None);
    }

    #[test]
    fn info_output_classifies_the_engine() {
        assert_eq!(
            classify_info_output(
                r#"{"host":{"buildahVersion":"1.37.0","ociRuntime":{"name":"crun"}}}"#
            ),
            Some(ContainerEngine::Podman)
        );
        assert_eq!(
            classify_info_output(r#"{"ID":"abc","ServerVersion":"27.3.1","Driver":"overlay2"}"#),
            Some(ContainerEngine::Docker)
        );
        assert_eq!(classify_info_output("{}"), None);
    }

    /// Write an executable fake CLI named `name` in a fresh temp dir, and run
    /// it once so a concurrent fork can't leave it ETXTBSY for the probe.
    #[cfg(unix)]
    fn fake_cli(name: &str, body: &str) -> (tempfile::TempDir, String) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut attempts = 0;
        loop {
            match std::process::Command::new(&path).arg("warmup").output() {
                Err(e) if e.kind() == std::io::ErrorKind::ExecutableFileBusy && attempts < 200 => {
                    attempts += 1;
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                _ => break,
            }
        }
        let cli = path.display().to_string();
        (dir, cli)
    }

    #[cfg(unix)]
    #[test]
    fn podman_docker_shim_is_detected_as_podman() {
        // What `podman-docker` installs: a `docker` that is podman.
        let (_dir, cli) = fake_cli(
            "docker",
            "[ \"$1\" = --version ] && { echo 'podman version 5.2.0'; exit 0; }\nexit 1\n",
        );
        assert!(is_podman(&cli));
        assert_eq!(probe_engine(&cli), Some(ContainerEngine::Podman));
    }

    #[cfg(unix)]
    #[test]
    fn real_docker_cli_is_not_podman() {
        let (_dir, cli) = fake_cli(
            "docker",
            "[ \"$1\" = --version ] && { echo 'Docker version 27.3.1, build ce12230'; exit 0; }\nexit 1\n",
        );
        assert!(!is_podman(&cli));
    }

    #[cfg(unix)]
    #[test]
    fn wrapper_with_opaque_version_falls_back_to_info() {
        let (_dir, cli) = fake_cli(
            "container-cli",
            "case \"$1\" in\n  --version) echo 'wrapper 1.0' ;;\n  info) echo '{\"host\":{\"buildahVersion\":\"1.37.0\"}}' ;;\n  *) exit 1 ;;\nesac\n",
        );
        assert!(is_podman(&cli));
    }

    #[cfg(unix)]
    #[test]
    fn engine_probe_is_cached_per_cli() {
        // Each run appends to a log; a second lookup must not run the CLI again.
        let (dir, cli) = fake_cli(
            "docker",
            "echo \"$*\" >> \"$(dirname \"$0\")/calls.log\"\n[ \"$1\" = --version ] && { echo 'podman version 5.2.0'; exit 0; }\nexit 1\n",
        );
        let log = dir.path().join("calls.log");
        let _ = std::fs::remove_file(&log);
        assert!(is_podman(&cli));
        assert!(is_podman(&cli));
        let calls = std::fs::read_to_string(&log).unwrap_or_default();
        assert_eq!(calls.lines().collect::<Vec<_>>(), ["--version"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn async_probe_detects_the_shim() {
        let (_dir, cli) = fake_cli(
            "docker",
            "[ \"$1\" = --version ] && { echo 'podman version 5.2.0'; exit 0; }\nexit 1\n",
        );
        assert!(is_podman_async(&cli).await);
    }

    #[cfg(unix)]
    #[test]
    fn engine_probe_bounds_a_hanging_cli() {
        // A CLI that never answers must not hang detection: one bounded call,
        // no `info` fallback against it, and it reads as Docker.
        // Only the probe's commands hang: `fake_cli`'s warmup run (no such
        // argument) must return at once, or it eats the test's time budget.
        let (_dir, cli) = fake_cli(
            "docker",
            "case \"$1\" in --version|info) sleep 600 ;; esac\nexit 1\n",
        );
        let start = std::time::Instant::now();
        assert!(!is_podman(&cli));
        let elapsed = start.elapsed();
        assert!(
            elapsed < CLI_PROBE_TIMEOUT + std::time::Duration::from_secs(5),
            "probe took {elapsed:?}, expected one bound near {CLI_PROBE_TIMEOUT:?}"
        );
    }

    #[test]
    fn missing_cli_is_not_podman() {
        // `FAKECLOUD_CONTAINER_CLI=false`-style sentinels and missing binaries
        // read as not-podman without error.
        assert!(!is_podman("definitely-not-a-real-cli-binary-fc-2599"));
        assert!(!is_podman("false"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_host_alias_treats_the_shim_as_podman() {
        let (_dir, cli) = fake_cli(
            "docker",
            "[ \"$1\" = --version ] && { echo 'podman version 5.2.0'; exit 0; }\nexit 1\n",
        );
        let (alias, add_host) = resolve_host_alias(&cli);
        assert_eq!(alias, "host.containers.internal");
        assert_eq!(add_host, None);
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
    #[cfg(unix)]
    fn resolve_host_alias_docker_emits_add_host() {
        // A fake Docker CLI, so a host whose `docker` is the podman-docker shim
        // can't flip this test.
        let (_dir, cli) = fake_cli(
            "docker",
            "[ \"$1\" = --version ] && { echo 'Docker version 27.3.1, build ce12230'; exit 0; }\nexit 1\n",
        );
        let (alias, add_host) = resolve_host_alias(&cli);
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
    fn only_objects_of_a_dead_owner_are_orphans() {
        let me = std::process::id();
        let alive = |pid: u32| pid == 4242;
        // Another live fakecloud process: never an orphan.
        assert!(!owned_by_dead_process("fakecloud-4242", alive));
        // Its owner is gone: an orphan.
        assert!(owned_by_dead_process("fakecloud-777", alive));
        // The current process, even if the probe says otherwise.
        assert!(!owned_by_dead_process(&format!("fakecloud-{me}"), |_| {
            false
        }));
        // Nothing proves an unparseable owner is gone.
        for label in ["", "fakecloud-", "fakecloud-abc", "other-777"] {
            assert!(!owned_by_dead_process(label, alive), "{label:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn pid_alive_probes_real_processes() {
        assert!(pid_alive(std::process::id()));
        assert!(!pid_alive(u32::MAX - 1));
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

#[cfg(test)]
mod bounded_cli_tests {
    use super::*;

    /// A wedged daemon leaves the CLI blocked on connect forever. Every
    /// container call has to end at the bound instead of hanging its caller,
    /// which for the reaper means hanging server startup.
    #[test]
    fn a_hanging_cli_call_is_cut_off() {
        let start = std::time::Instant::now();
        let mut child = spawn_bounded(
            std::process::Command::new("sleep")
                .arg("600")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null()),
        )
        .expect("sleep is available");
        assert!(!wait_bounded_group(&mut child));
        assert!(
            start.elapsed() < CLI_PROBE_TIMEOUT + std::time::Duration::from_secs(5),
            "the wait must end at the bound"
        );
    }

    /// Output larger than a pipe buffer (64 KiB on Linux) must come back
    /// whole. Waiting for the child to exit before reading blocks it on write
    /// forever, so this used to burn the full timeout and report failure.
    #[test]
    fn output_larger_than_the_pipe_buffer_still_comes_back() {
        let start = std::time::Instant::now();
        // 200_000 bytes: comfortably past the buffer on every supported host.
        let out = bounded_output("sh", &["-c", "printf 'x%.0s' $(seq 1 200000)"])
            .expect("a large but prompt call must succeed");
        assert_eq!(out.len(), 200_000, "output was truncated");
        assert!(
            start.elapsed() < CLI_PROBE_TIMEOUT,
            "a prompt call must not reach the deadline"
        );
    }

    #[test]
    fn a_prompt_cli_call_returns_its_output() {
        assert_eq!(
            bounded_output("echo", &["abc123"])
                .as_deref()
                .map(str::trim),
            Some("abc123")
        );
        assert!(bounded_status("true", &[]));
        assert!(!bounded_status("false", &[]));
    }

    /// A bounded call gets an empty stdin, never fakecloud's own. The process
    /// group `spawn_bounded` creates is a *background* one, so a child that
    /// reads the controlling terminal -- a `sudo`/credential-helper wrapper
    /// prompting -- is stopped by SIGTTIN, which the WNOHANG `try_wait` loop
    /// cannot see: the call would burn the whole deadline and be killed.
    /// `cat` with no argument reads stdin to EOF, so it returns at once with
    /// nothing only when stdin is /dev/null.
    #[cfg(unix)]
    #[test]
    fn a_bounded_call_reads_an_empty_stdin() {
        let start = std::time::Instant::now();
        let out = bounded_output("cat", &[]).expect("a call reading stdin must not time out");
        assert!(out.is_empty(), "stdin must be empty, got {out:?}");
        assert!(
            start.elapsed() < CLI_PROBE_TIMEOUT,
            "a call reading stdin must not reach the deadline"
        );
    }

    /// The happy path must still hand back the child's output *and* collect the
    /// reader, so the no-leak guarantee isn't bought by dropping output.
    #[test]
    fn a_prompt_cli_call_collects_its_reader() {
        let (output, reader) = run_bounded("echo", &["abc123"]);
        assert_eq!(output.as_deref().map(str::trim), Some("abc123"));
        assert!(
            matches!(reader, ReaderState::Finished),
            "reader was {reader:?}, expected it collected"
        );
    }

    /// The bridge-gateway probe talks to the same daemon as the liveness probe,
    /// so it has to end at the same bound. It used to be a plain
    /// `Command::output()`, which against a wedged daemon hung whichever runtime
    /// constructor called it -- on Linux, every container-backed service at
    /// server startup. Timing out is not an error here: the caller falls back to
    /// the conventional `172.17.0.1`.
    #[cfg(unix)]
    #[test]
    fn a_hanging_bridge_gateway_probe_is_cut_off() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("fc-gwtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let script = dir.join("hangcli");
        std::fs::write(&script, "#!/bin/sh\nsleep 600\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let start = std::time::Instant::now();
        let gateway = detect_bridge_gateway(script.to_str().unwrap());
        let elapsed = start.elapsed();

        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(gateway, None, "a wedged daemon must report no gateway");
        assert!(
            elapsed < CLI_PROBE_TIMEOUT + READER_DRAIN_GRACE + std::time::Duration::from_secs(5),
            "the probe took {elapsed:?}, expected it bounded near {CLI_PROBE_TIMEOUT:?}"
        );
    }

    /// The unavailable-CLI path keeps its shape: nothing to spawn, no gateway,
    /// and the caller's fallback stands.
    #[test]
    fn a_missing_cli_reports_no_bridge_gateway() {
        assert_eq!(
            detect_bridge_gateway("definitely-not-a-real-cli-binary-xyz-123"),
            None
        );
    }

    /// A CLI that succeeds with no output -- an `inspect --format` over a bridge
    /// with no IPAM config -- still means "no gateway", not an empty
    /// `--add-host` value. Unchanged by the bounding; guarded so it stays that
    /// way.
    #[test]
    fn an_empty_gateway_is_rejected() {
        assert_eq!(detect_bridge_gateway("true"), None);
    }

    /// `FAKECLOUD_CONTAINER_CLI` is routinely a wrapper (`sh -c 'exec docker
    /// "$@"'`, a `podman-remote` shim), which makes the real command a
    /// grandchild holding the stdout pipe. Killing only the direct child left
    /// the reader's `read_to_end` blocked forever -- a thread parked for the
    /// life of the process, once per call, on exactly the wedged-daemon path
    /// these bounds were added for (the server reaper calls this at startup).
    /// The reader reporting in is the evidence: EOF on that pipe is only
    /// possible once every write end is closed, so a collected buffer proves
    /// the grandchildren went down with the call.
    #[cfg(unix)]
    #[test]
    fn a_timed_out_wrapper_call_leaves_no_reader_behind() {
        let start = std::time::Instant::now();
        // A wrapper that outlives its own kill: the backgrounded sleep inherits
        // the stdout pipe and is not the process we spawned.
        let (output, reader) = run_bounded("sh", &["-c", "sleep 600 & sleep 600"]);
        assert_eq!(output, None, "a wedged call must report failure");
        assert!(
            matches!(reader, ReaderState::Finished),
            "reader was {reader:?}: the stdout reader must not outlive the call"
        );
        assert!(
            start.elapsed()
                < CLI_PROBE_TIMEOUT + READER_DRAIN_GRACE + std::time::Duration::from_secs(5),
            "the call must still end at the bound, took {:?}",
            start.elapsed()
        );
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn ecr_registry_host_defaults_to_loopback() {
        // The host daemon performs the pull; the sibling alias is wrong there.
        assert_eq!(resolve_ecr_registry_host(None, false, true), "127.0.0.1");
        assert_eq!(resolve_ecr_registry_host(None, false, false), "127.0.0.1");
        assert_eq!(
            resolve_ecr_registry_host(Some("  ".into()), false, true),
            "127.0.0.1"
        );
        // Podman on Linux pulls on the host; podman machine pulls in its VM.
        assert_eq!(resolve_ecr_registry_host(None, true, true), "127.0.0.1");
        assert_eq!(
            resolve_ecr_registry_host(None, true, false),
            "host.containers.internal"
        );
        assert_eq!(
            resolve_ecr_registry_host(Some("10.0.0.5".into()), true, false),
            "10.0.0.5"
        );
    }

    #[test]
    fn published_port_parses_docker_and_podman_output() {
        assert_eq!(
            parse_published_port("0.0.0.0:49153\n[::]:49153\n"),
            Some(49153)
        );
        assert_eq!(parse_published_port("[::]:5000"), Some(5000));
        assert_eq!(parse_published_port(""), None);
        assert_eq!(parse_published_port("garbage"), None);
    }

    #[test]
    fn rewrite_loopback_urls_and_bare_endpoints() {
        let h = "host.docker.internal";
        assert_eq!(
            rewrite_loopback_value("http://localhost:4566", h),
            "http://host.docker.internal:4566"
        );
        assert_eq!(
            rewrite_loopback_value("https://127.0.0.1:4566/path", h),
            "https://host.docker.internal:4566/path"
        );
        // Bare host (an RDS / ElastiCache endpoint address).
        assert_eq!(rewrite_loopback_value("127.0.0.1", h), h);
        assert_eq!(rewrite_loopback_value("localhost", h), h);
        // Bare host:port and comma-separated broker lists.
        assert_eq!(
            rewrite_loopback_value("127.0.0.1:6379", h),
            "host.docker.internal:6379"
        );
        assert_eq!(
            rewrite_loopback_value("127.0.0.1:9092,localhost:9094", h),
            "host.docker.internal:9092,host.docker.internal:9094"
        );
        // Scheme-qualified DSNs with userinfo.
        assert_eq!(
            rewrite_loopback_value("postgres://u:p@localhost:5432/db", h),
            "postgres://u:p@host.docker.internal:5432/db"
        );
        assert_eq!(
            rewrite_loopback_value("redis://127.0.0.1:6379/0", h),
            "redis://host.docker.internal:6379/0"
        );
        // URL host without a port.
        assert_eq!(
            rewrite_loopback_value("http://localhost/health", h),
            "http://host.docker.internal/health"
        );
        // key=value lists.
        assert_eq!(
            rewrite_loopback_value("host=127.0.0.1:5432 user=x", h),
            "host=host.docker.internal:5432 user=x"
        );
    }

    #[test]
    fn rewrite_loopback_leaves_prose_and_lookalikes_alone() {
        let h = "host.docker.internal";
        for v in [
            "EHLO localhost",
            "connect to localhost soon",
            "127.0.0.10:80",
            "localhost.localdomain:25",
            "mylocalhost:80",
            "",
        ] {
            assert_eq!(rewrite_loopback_value(v, h), v, "{v:?}");
        }
        // Workload sharing fakecloud's namespace: nothing to rewrite.
        assert_eq!(
            rewrite_loopback_value("http://localhost:4566", "127.0.0.1"),
            "http://localhost:4566"
        );
    }

    #[test]
    fn rewrite_loopback_swaps_registered_container_ports() {
        // A broker whose host listener advertises loopback registers the
        // listener containers must use instead.
        crate::dataplane::register_container_port(41001, 41002);
        assert_eq!(
            rewrite_loopback_value("127.0.0.1:41001", "host.docker.internal"),
            "host.docker.internal:41002"
        );
        crate::dataplane::unregister_container_port(41001);
        assert_eq!(
            rewrite_loopback_value("127.0.0.1:41001", "host.docker.internal"),
            "host.docker.internal:41001"
        );
    }
}
