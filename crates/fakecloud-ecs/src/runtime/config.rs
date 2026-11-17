//! `EcsRuntime` `config` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcsRuntime {
    /// Path suitable for `DOCKER_CONFIG`. `None` if the tempdir setup
    /// failed; in that case pulls fall back to the user's own config and
    /// will only work if they've already logged in.
    pub(super) fn docker_config_path(&self) -> Option<PathBuf> {
        self.docker_config.as_ref().map(|d| d.path().to_path_buf())
    }

    /// Build a `Command` for the container CLI with `DOCKER_CONFIG` set
    /// to our isolated tempdir so fakecloud ECR auth works out of the box.
    pub(super) fn cli_command(&self) -> Command {
        let mut cmd = Command::new(&self.cli);
        if let Some(p) = self.docker_config_path() {
            cmd.env("DOCKER_CONFIG", p);
        }
        cmd
    }

    /// The container CLI as a cheap, cloneable handle, for work that outlives
    /// a borrow of the runtime (detached cleanups, background execs).
    pub(crate) fn container_cli(&self) -> ContainerCli {
        ContainerCli(Arc::from(self.cli.as_str()))
    }

    /// Run the container CLI with `args`, bounded by `timeout`; see
    /// [`ContainerCli::output`].
    pub(super) async fn cli_output<I, S>(
        &self,
        args: I,
        timeout: Duration,
    ) -> Result<std::process::Output, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.container_cli().output(args, timeout).await
    }

    /// Start an `ExecuteCommand` command (`argv`, run as given, no shell) in
    /// task `task_id`'s container `container_id` and return at once, as ECS
    /// does: the API opens a Session Manager session and returns it, and the
    /// command runs in that session, never in the API call. fakecloud has no
    /// session client, so nothing streams the command's output; its exit
    /// status and output are logged.
    ///
    /// The command is bounded inside the container by `timeout` -- Session
    /// Manager ends a session nobody interacts with at its idle timeout,
    /// terminating its process -- by running it under the image's `timeout`
    /// (GNU coreutils or busybox). An image without one (distroless, scratch)
    /// runs it directly; then only the exec client on the host is killed at
    /// the deadline, and the command itself runs until it exits or the task
    /// stops. Either way it ends when the task stops: StopTask and shutdown
    /// abort it (see [`Self::abort_execs`]).
    ///
    /// Refused when the task or the host already runs the maximum number of
    /// commands, and on the Kubernetes backend, which has no exec path.
    pub(crate) fn spawn_exec_command(
        self: &Arc<Self>,
        task_id: &str,
        container_id: &str,
        argv: Vec<String>,
        timeout: Duration,
    ) -> Result<(), String> {
        if self.k8s.is_some() {
            return Err("ExecuteCommand is not supported by the Kubernetes task backend".into());
        }
        let mut sessions = self.exec_sessions.lock();
        for handles in sessions.values_mut() {
            handles.retain(|h| !h.is_finished());
        }
        sessions.retain(|_, handles| !handles.is_empty());
        let in_task = sessions.get(task_id).map_or(0, Vec::len);
        if in_task >= MAX_EXECS_PER_TASK {
            return Err(format!(
                "Task {task_id} already has {MAX_EXECS_PER_TASK} commands running from \
                 ExecuteCommand; wait for one to finish"
            ));
        }
        if sessions.values().map(Vec::len).sum::<usize>() >= MAX_EXECS {
            return Err(format!(
                "{MAX_EXECS} commands from ExecuteCommand are already running; wait for one \
                 to finish"
            ));
        }
        let cli = self.container_cli();
        let (task, container) = (task_id.to_string(), container_id.to_string());
        let handle = tokio::spawn(async move {
            match run_exec_command(&cli, &container, &argv, timeout).await {
                Ok(out) => tracing::info!(
                    task = %task,
                    exit = out.status.code().unwrap_or(-1),
                    stdout = %String::from_utf8_lossy(&out.stdout).trim(),
                    stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                    "ExecuteCommand finished"
                ),
                Err(error) => tracing::warn!(task = %task, %error, "ExecuteCommand failed"),
            }
        });
        sessions
            .entry(task_id.to_string())
            .or_default()
            .push(handle);
        Ok(())
    }

    /// Abort every `ExecuteCommand` command still running for `task_id`
    /// (every task when `None`). Dropping a command's CLI call kills the
    /// client's whole process group (see `output_within`).
    pub(crate) fn abort_execs(&self, task_id: Option<&str>) {
        let mut sessions = self.exec_sessions.lock();
        let aborted: Vec<_> = match task_id {
            Some(id) => sessions.remove(id).into_iter().flatten().collect(),
            None => sessions.drain().flat_map(|(_, h)| h).collect(),
        };
        for handle in aborted {
            handle.abort();
        }
    }

    /// `ExecuteCommand` commands of `task_id` still running.
    #[cfg(test)]
    pub(crate) fn running_execs(&self, task_id: &str) -> usize {
        self.exec_sessions
            .lock()
            .get(task_id)
            .map_or(0, |h| h.iter().filter(|h| !h.is_finished()).count())
    }

    /// Wire SecretsManager state so `secrets[].valueFrom` entries
    /// pointing at SecretsManager ARNs resolve at task launch.
    pub fn with_secretsmanager(mut self, state: SharedSecretsManagerState) -> Self {
        self.secretsmanager_state = Some(state);
        self
    }

    /// Wire SSM state so `secrets[].valueFrom` entries pointing at
    /// Parameter Store ARNs resolve at task launch.
    pub fn with_ssm(mut self, state: SharedSsmState) -> Self {
        self.ssm_state = Some(state);
        self
    }

    /// Wire the KMS hook so `secrets[]` values stored KMS-encrypted (SSM
    /// `SecureString`s, secrets with a KMS key) are decrypted at task launch.
    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }
}

/// The container CLI's program name, cloneable into detached tasks so they run
/// it exactly as [`EcsRuntime::cli_output`] does.
#[derive(Debug, Clone)]
pub(crate) struct ContainerCli(Arc<str>);

impl ContainerCli {
    /// Run the CLI with `args`, killed (with its process group) once it
    /// outruns `timeout` or the call is dropped. A daemon or registry that
    /// stops answering leaves the CLI blocked with no deadline of its own,
    /// which would hold a task in `PENDING`, or a stopped task short of
    /// `STOPPED`, forever. The error names the subcommand.
    pub(crate) async fn output<I, S>(
        &self,
        args: I,
        timeout: Duration,
    ) -> Result<std::process::Output, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let args: Vec<std::ffi::OsString> =
            args.into_iter().map(|a| a.as_ref().to_owned()).collect();
        let verb = args
            .first()
            .map(|a| a.to_string_lossy().into_owned())
            .unwrap_or_default();
        fakecloud_core::container_net::output_within(Command::new(&*self.0).args(&args), timeout)
            .await
            .map_err(|e| format!("{} {verb} {e}", self.0))
    }
}

/// Grace the exec client gets past the in-container `timeout`, so the
/// command's own deadline ends the exec and the client exits on its own.
const EXEC_CLIENT_GRACE: Duration = Duration::from_secs(30);

/// Run `argv` in `container` under the image's `timeout`, or directly when the
/// image has none (see [`EcsRuntime::spawn_exec_command`]).
async fn run_exec_command(
    cli: &ContainerCli,
    container: &str,
    argv: &[String],
    timeout: Duration,
) -> Result<std::process::Output, String> {
    let secs = timeout.as_secs().max(1).to_string();
    let bounded = ["exec", container, "timeout", &secs]
        .into_iter()
        .chain(argv.iter().map(String::as_str));
    let out = cli.output(bounded, timeout + EXEC_CLIENT_GRACE).await?;
    if !lacks_timeout_binary(&out) {
        return Ok(out);
    }
    tracing::info!(
        container = %container,
        "no `timeout` in the container; running the ExecuteCommand command unbounded inside it"
    );
    let direct = ["exec", container]
        .into_iter()
        .chain(argv.iter().map(String::as_str));
    cli.output(direct, timeout).await
}

/// Whether an exec failed because the container has no `timeout` to run --
/// the engine's own "executable not found" for it (Docker / runc, Podman /
/// crun), never the command's output, so a command that did run is never
/// run again.
fn lacks_timeout_binary(out: &std::process::Output) -> bool {
    if !matches!(out.status.code(), Some(126 | 127)) {
        return false;
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    stderr.contains("\"timeout\": executable file not found")
        || stderr.contains("executable file `timeout` not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::tests_support::{exec_calls, exec_cli, exec_pids};
    use fakecloud_core::test_support::{pid_gone, wait_until};

    fn runtime(cli: &str) -> Arc<EcsRuntime> {
        Arc::new(EcsRuntime::with_cli_for_tests(cli))
    }

    fn argv(cmd: &str) -> Vec<String> {
        cmd.split(' ').map(String::from).collect()
    }

    /// The command runs under the container's own `timeout`, so it ends at
    /// its deadline inside the container, and the call returns at once.
    // Multi-threaded: the test blocks while polling for the background exec.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exec_runs_under_the_containers_timeout() {
        let (dir, cli) = exec_cli(true);
        let rt = runtime(&cli);
        let start = std::time::Instant::now();
        rt.spawn_exec_command("t1", "c1", argv("sleep infinity"), Duration::from_secs(1))
            .expect("docker backend execs");
        assert!(start.elapsed() < Duration::from_millis(200));
        assert!(
            wait_until(Duration::from_secs(10), || rt.running_execs("t1") == 0),
            "the in-container timeout did not end the command"
        );
        assert_eq!(exec_calls(&dir), ["exec c1 timeout 1 sleep infinity"]);
    }

    /// An image with no `timeout` (distroless) runs the command directly; the
    /// exec client is then killed at the deadline.
    // Multi-threaded: the test blocks while polling for the background exec.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exec_without_a_timeout_binary_runs_directly_and_is_cut_off() {
        let (dir, cli) = exec_cli(false);
        let rt = runtime(&cli);
        rt.spawn_exec_command(
            "t1",
            "c1",
            argv("sleep infinity"),
            Duration::from_millis(500),
        )
        .expect("docker backend execs");
        let pids = exec_pids(&dir, 1);
        assert!(
            wait_until(Duration::from_secs(10), || rt.running_execs("t1") == 0),
            "the exec outlived its deadline"
        );
        assert_eq!(
            exec_calls(&dir),
            ["exec c1 timeout 1 sleep infinity", "exec c1 sleep infinity"]
        );
        assert!(pid_gone(pids[0]), "exec process {} survived", pids[0]);
    }

    /// StopTask ends the task's commands, as ECS ends its sessions; other
    /// tasks' commands keep running until shutdown ends them too.
    // Multi-threaded: the test blocks while polling for the background exec.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_task_and_shutdown_abort_running_execs() {
        let (dir, cli) = exec_cli(false);
        let rt = runtime(&cli);
        for task in ["t1", "t2"] {
            rt.spawn_exec_command(task, "c1", argv("sleep infinity"), Duration::from_secs(600))
                .unwrap();
        }
        let pids = exec_pids(&dir, 2);
        let state: crate::state::SharedEcsState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "000000000000",
                "us-east-1",
                "http://localhost:4566",
            ),
        ));
        rt.stop_task(&state, "000000000000", "t1", "test").await;
        assert_eq!(rt.running_execs("t1"), 0);
        assert_eq!(rt.running_execs("t2"), 1);
        assert!(
            wait_until(Duration::from_secs(5), || pids
                .iter()
                .filter(|&&p| fakecloud_core::container_net::pid_alive(p))
                .count()
                == 1),
            "StopTask must kill exactly its own task's exec"
        );
        rt.stop_all().await;
        assert_eq!(rt.running_execs("t2"), 0);
        for pid in pids {
            assert!(pid_gone(pid), "exec process {pid} survived shutdown");
        }
    }

    /// Commands are capped per task and across tasks; finished ones free
    /// their slot.
    // Multi-threaded: the test blocks while polling for the background exec.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_execs_are_capped_per_task_and_overall() {
        let (dir, cli) = exec_cli(false);
        let rt = runtime(&cli);
        let long = Duration::from_secs(600);
        for _ in 0..MAX_EXECS_PER_TASK {
            rt.spawn_exec_command("t0", "c", argv("sleep infinity"), long)
                .unwrap();
        }
        let err = rt
            .spawn_exec_command("t0", "c", argv("sleep infinity"), long)
            .expect_err("per-task cap");
        assert!(err.contains("already has 8 commands running"), "{err}");
        let mut task = 1;
        while rt
            .exec_sessions
            .lock()
            .values()
            .map(Vec::len)
            .sum::<usize>()
            < MAX_EXECS
        {
            rt.spawn_exec_command(
                &format!("t{}", task / MAX_EXECS_PER_TASK + 1),
                "c",
                argv("x"),
                long,
            )
            .unwrap();
            task += 1;
        }
        let err = rt
            .spawn_exec_command("fresh", "c", argv("sleep infinity"), long)
            .expect_err("global cap");
        assert!(err.contains("64 commands"), "{err}");
        let pids = exec_pids(&dir, MAX_EXECS);
        rt.abort_execs(Some("t0"));
        rt.spawn_exec_command("fresh", "c", argv("sleep infinity"), long)
            .expect("an aborted task frees its slots");
        rt.stop_all().await;
        for pid in pids {
            assert!(pid_gone(pid), "exec process {pid} survived shutdown");
        }
    }

    #[test]
    fn only_the_engines_own_not_found_means_no_timeout_binary() {
        use std::os::unix::process::ExitStatusExt;
        let out = |code: i32, stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        };
        // Docker (runc).
        assert!(lacks_timeout_binary(&out(
            126,
            r#"OCI runtime exec failed: exec failed: unable to start container process: exec: "timeout": executable file not found in $PATH: unknown"#
        )));
        // Podman (crun).
        assert!(lacks_timeout_binary(&out(
            127,
            "Error: crun: executable file `timeout` not found in $PATH: No such file or directory: OCI runtime attempted to invoke a command that was not found"
        )));
        // The command itself is missing: `timeout` ran, so does the image's.
        assert!(!lacks_timeout_binary(&out(
            127,
            "timeout: failed to run command 'nope': No such file or directory"
        )));
        // A command that ran and failed is never re-run.
        assert!(!lacks_timeout_binary(&out(
            1,
            r#"exec: "timeout": executable file not found"#
        )));
    }
}
