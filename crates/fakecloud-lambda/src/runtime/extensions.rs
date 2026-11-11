//! External extensions (`/opt/extensions/*`) and the launcher that starts
//! them.
//!
//! On Lambda the platform starts every executable in `/opt/extensions`
//! before the runtime: extensions register with the Extensions API (served
//! with the Runtime API, see [`super::api`]) during the extension init phase,
//! and only then does the runtime start, so function init code can already
//! use them (the Parameters and Secrets extension on `localhost:2773`, say).
//!
//! Every backend runs a function's container (zip or image) with
//! [`LAUNCHER_SCRIPT`] as its entrypoint and the function's own entrypoint
//! and command as arguments. The launcher starts each extension with the
//! container's stdout/stderr, records how many it started and how any of
//! them exits, waits until fakecloud has seen them all register, then execs
//! the original entrypoint. fakecloud reads and writes those files through
//! the backend (see [`super::backend::LambdaBackend::read_instance_file`]).

/// Where the launcher lives in the container, with its bookkeeping files.
pub(crate) const LAUNCHER_DIR: &str = "/var/fakecloud";
pub(crate) const LAUNCHER_PATH: &str = "/var/fakecloud/launch";
/// Written once all extensions are started: how many.
pub(crate) const LAUNCHED_FILE: &str = "/var/fakecloud/extensions.launched";
/// One `exit <status> <name>` line per extension that ended.
pub(crate) const EXITED_FILE: &str = "/var/fakecloud/extensions.exited";
/// Created by fakecloud once every extension has registered; the runtime
/// starts then.
pub(crate) const GO_FILE: &str = "/var/fakecloud/runtime.go";

/// The base images' entrypoint, which runs the runtime.
pub(crate) const BASE_IMAGE_ENTRYPOINT: &str = "/lambda-entrypoint.sh";

/// The launcher (`/bin/sh`). Starts each regular executable file in
/// `/opt/extensions` (symlinks followed, hard links included) with the
/// container's stdio, then runs `"$@"` (the function's entrypoint and
/// command) in its own place once fakecloud has seen the extensions
/// register.
pub(crate) const LAUNCHER_SCRIPT: &str = r#"#!/bin/sh
d=/var/fakecloud
n=0
for e in /opt/extensions/*; do
  if [ -f "$e" ] && [ -x "$e" ]; then
    n=$((n+1))
    ( "$e"; echo "exit $? ${e##*/}" >> "$d/extensions.exited" ) &
  fi
done
echo "$n" > "$d/extensions.launched.tmp" && mv "$d/extensions.launched.tmp" "$d/extensions.launched"
if [ "$n" -gt 0 ]; then
  while [ ! -e "$d/runtime.go" ]; do sleep 0.02; done
fi
exec "$@"
"#;

/// The command a function's container runs: its entrypoint followed by its
/// command, from the function's `ImageConfig` where set (as Lambda applies
/// it), else the image's own.
pub(crate) fn function_command(
    image_entrypoint: &[String],
    image_cmd: &[String],
    image_config: Option<&serde_json::Value>,
) -> Vec<String> {
    let list = |key: &str| -> Option<Vec<String>> {
        image_config?
            .get(key)?
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .filter(|v: &Vec<String>| !v.is_empty())
    };
    let mut command = list("EntryPoint").unwrap_or_else(|| image_entrypoint.to_vec());
    command.extend(list("Command").unwrap_or_else(|| image_cmd.to_vec()));
    command
}

/// The working directory `ImageConfig` sets, if any.
pub(crate) fn image_working_directory(image_config: Option<&serde_json::Value>) -> Option<String> {
    image_config?
        .get("WorkingDirectory")?
        .as_str()
        .filter(|w| !w.is_empty())
        .map(str::to_string)
}

/// The `(name, exit status)` of each extension in the launcher's exit file.
pub(crate) fn exited_extensions(contents: &str) -> Vec<(String, i32)> {
    contents
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("exit ")?;
            let (code, name) = rest.split_once(' ')?;
            Some((name.trim().to_string(), code.trim().parse().ok()?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_config_overrides_the_image_command() {
        let entry = vec!["/lambda-entrypoint.sh".to_string()];
        let cmd = vec!["app.handler".to_string()];
        assert_eq!(
            function_command(&entry, &cmd, None),
            vec!["/lambda-entrypoint.sh", "app.handler"]
        );
        let config = serde_json::json!({"Command": ["other.handler"], "WorkingDirectory": "/srv"});
        assert_eq!(
            function_command(&entry, &cmd, Some(&config)),
            vec!["/lambda-entrypoint.sh", "other.handler"]
        );
        let config = serde_json::json!({"EntryPoint": ["/bin/app"], "Command": []});
        assert_eq!(
            function_command(&entry, &cmd, Some(&config)),
            vec!["/bin/app", "app.handler"]
        );
        assert_eq!(
            image_working_directory(Some(&serde_json::json!({"WorkingDirectory": "/srv"})))
                .as_deref(),
            Some("/srv")
        );
        assert_eq!(image_working_directory(None), None);
    }

    #[test]
    fn exit_lines_parse() {
        assert_eq!(
            exited_extensions("exit 1 agent\nexit 127 broken\ngarbage\n"),
            vec![("agent".to_string(), 1), ("broken".to_string(), 127)]
        );
    }

    /// The launcher itself, run by a real shell: it starts executables
    /// (following symlinks), skips the rest, records the count and exits,
    /// waits for the go file, and runs the command with its arguments.
    #[test]
    fn launcher_starts_extensions_first_then_execs_the_command() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let ext = root.path().join("opt/extensions");
        let state = root.path().join("var/fakecloud");
        std::fs::create_dir_all(&ext).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        let script = LAUNCHER_SCRIPT
            .replace("/opt/extensions", ext.to_str().unwrap())
            .replace("/var/fakecloud", state.to_str().unwrap());
        let write = |name: &str, body: &str, mode: u32| {
            let p = ext.join(name);
            std::fs::write(&p, body).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        write("good", "#!/bin/sh\necho good-ran\nexit 3\n", 0o755);
        write("notes.txt", "not an extension", 0o644);
        std::os::unix::fs::symlink(ext.join("good"), ext.join("linked")).unwrap();
        // The go file is already there, so the launcher runs straight on.
        std::fs::write(state.join("runtime.go"), "").unwrap();
        let out = std::process::Command::new("/bin/sh")
            .args(["-c", &script, "launcher", "/bin/echo", "runtime", "arg"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("runtime arg"), "{stdout}");
        assert_eq!(
            std::fs::read_to_string(state.join("extensions.launched"))
                .unwrap()
                .trim(),
            "2"
        );
        // Both extension subshells finish and record their exit.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let exited =
                std::fs::read_to_string(state.join("extensions.exited")).unwrap_or_default();
            let mut names: Vec<String> = exited_extensions(&exited)
                .into_iter()
                .map(|(n, c)| {
                    assert_eq!(c, 3);
                    n
                })
                .collect();
            names.sort();
            if names == ["good", "linked"] {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{exited:?}");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}
