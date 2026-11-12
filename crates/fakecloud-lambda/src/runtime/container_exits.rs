//! Exit notifications for Docker/Podman Lambda containers.
//!
//! A function's runtime is the container's main process, so the container
//! dying is the runtime exiting: the facade needs to know at once to report
//! `Runtime.ExitError` (or retry an event the runtime never took) instead of
//! waiting out the timeout. One `<cli> events` stream per backend, filtered to
//! this fakecloud's containers, feeds a watch per container; a stream that
//! ends is restarted with `--since` far enough back to replay what happened
//! meanwhile.

use std::collections::{HashMap, HashSet};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::AsyncBufReadExt;
use tokio::sync::watch;

use super::backend::{ExitWatch, InstanceExit};

/// How far back a (re)started event stream replays. Covers a daemon clock a
/// little behind the host's and the gap while a stream restarts.
const REPLAY: chrono::Duration = chrono::Duration::seconds(60);

/// One container event that matters here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContainerEvent {
    /// The container's main process ended with this exit code.
    Died { id: String, code: Option<i64> },
    /// The kernel OOM-killed a process in the container.
    Oom { id: String },
    /// The container was removed.
    Removed { id: String },
}

/// Parse one line of `<cli> events --format '{{json .}}'` (Docker's
/// `{"Action":"die","Actor":{"ID":..,"Attributes":{"exitCode":"3"}}}` or
/// Podman's `{"Status":"died","ID":..,"ContainerExitCode":3}`).
pub(crate) fn parse_event(line: &str) -> Option<ContainerEvent> {
    let v: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    if let Some(t) = v.get("Type").and_then(|t| t.as_str()) {
        if t != "container" {
            return None;
        }
    }
    let action = v
        .get("Action")
        .or_else(|| v.get("status"))
        .or_else(|| v.get("Status"))
        .and_then(|a| a.as_str())?
        .to_ascii_lowercase();
    let id = v
        .pointer("/Actor/ID")
        .or_else(|| v.get("id"))
        .or_else(|| v.get("ID"))
        .and_then(|i| i.as_str())?
        .to_string();
    let code = v
        .pointer("/Actor/Attributes/exitCode")
        .and_then(|c| match c {
            serde_json::Value::String(s) => s.parse().ok(),
            other => other.as_i64(),
        })
        .or_else(|| v.get("ContainerExitCode").and_then(|c| c.as_i64()));
    match action.as_str() {
        "die" | "died" => Some(ContainerEvent::Died { id, code }),
        "oom" => Some(ContainerEvent::Oom { id }),
        "destroy" | "remove" => Some(ContainerEvent::Removed { id }),
        _ => None,
    }
}

#[derive(Default)]
struct Watched {
    senders: HashMap<String, watch::Sender<Option<InstanceExit>>>,
    oom: HashSet<String>,
}

/// Exit watches for the containers one backend launched.
pub(crate) struct ContainerExits {
    cli: String,
    /// `key=value` label every watched container carries.
    label: String,
    watched: Mutex<Watched>,
    stream: OnceLock<()>,
}

impl ContainerExits {
    pub(crate) fn new(cli: String, label: String) -> Arc<Self> {
        Arc::new(Self {
            cli,
            label,
            watched: Mutex::new(Watched::default()),
            stream: OnceLock::new(),
        })
    }

    /// Watch container `id` (full ID, as `create` printed it). Call before
    /// starting it.
    pub(crate) fn watch(self: &Arc<Self>, id: &str) -> ExitWatch {
        self.ensure_stream();
        let (tx, rx) = watch::channel(None);
        self.watched.lock().senders.insert(id.to_string(), tx);
        rx
    }

    /// Stop watching container `id`.
    pub(crate) fn forget(&self, id: &str) {
        let mut w = self.watched.lock();
        w.senders.remove(id);
        w.oom.remove(id);
    }

    /// Apply one event to the watched containers.
    pub(crate) fn apply(&self, event: ContainerEvent) {
        let mut w = self.watched.lock();
        // Events name containers by their full ID; match a watch registered
        // under either form.
        let key = |w: &Watched, id: &str| {
            w.senders
                .keys()
                .find(|k| k.starts_with(id) || id.starts_with(k.as_str()))
                .cloned()
        };
        match event {
            ContainerEvent::Oom { id } => {
                if let Some(k) = key(&w, &id) {
                    w.oom.insert(k);
                }
            }
            ContainerEvent::Died { id, code } => {
                if let Some(k) = key(&w, &id) {
                    let oom = w.oom.contains(&k);
                    let exit = InstanceExit::from_container_code(code.unwrap_or(0), oom);
                    if let Some(tx) = w.senders.get(&k) {
                        tx.send_if_modified(|cur| {
                            if cur.is_none() {
                                *cur = Some(exit);
                                true
                            } else {
                                false
                            }
                        });
                    }
                }
            }
            ContainerEvent::Removed { id } => {
                if let Some(k) = key(&w, &id) {
                    if let Some(tx) = w.senders.get(&k) {
                        tx.send_if_modified(|cur| {
                            if cur.is_none() {
                                *cur = Some(InstanceExit::default());
                                true
                            } else {
                                false
                            }
                        });
                    }
                }
            }
        }
    }

    fn ensure_stream(self: &Arc<Self>) {
        if self.stream.set(()).is_err() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                let since = (chrono::Utc::now() - REPLAY)
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string();
                let child = tokio::process::Command::new(&this.cli)
                    .args([
                        "events",
                        "--filter",
                        "type=container",
                        "--filter",
                        &format!("label={}", this.label),
                        "--since",
                        &since,
                        "--format",
                        "{{json .}}",
                    ])
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn();
                drop(this);
                match child {
                    Ok(mut child) => {
                        if let Some(stdout) = child.stdout.take() {
                            let mut lines = tokio::io::BufReader::new(stdout).lines();
                            while let Ok(Some(line)) = lines.next_line().await {
                                let Some(this) = weak.upgrade() else { return };
                                if let Some(event) = parse_event(&line) {
                                    this.apply(event);
                                }
                            }
                        }
                        let _ = child.kill().await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "could not watch Lambda container exits");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_docker_and_podman_events() {
        let docker = r#"{"status":"die","id":"abc123","from":"img","Type":"container","Action":"die","Actor":{"ID":"abc123","Attributes":{"exitCode":"3","fakecloud-instance":"x"}},"scope":"local","time":1}"#;
        assert_eq!(
            parse_event(docker),
            Some(ContainerEvent::Died {
                id: "abc123".into(),
                code: Some(3)
            })
        );
        let podman = r#"{"ID":"def456","Image":"img","Name":"n","Status":"died","Time":"2026-01-01T00:00:00Z","Type":"container","ContainerExitCode":137,"Attributes":{}}"#;
        assert_eq!(
            parse_event(podman),
            Some(ContainerEvent::Died {
                id: "def456".into(),
                code: Some(137)
            })
        );
        assert_eq!(
            parse_event(r#"{"Type":"container","Action":"oom","Actor":{"ID":"a"}}"#),
            Some(ContainerEvent::Oom { id: "a".into() })
        );
        assert_eq!(
            parse_event(r#"{"Type":"container","Action":"destroy","Actor":{"ID":"a"}}"#),
            Some(ContainerEvent::Removed { id: "a".into() })
        );
        assert_eq!(
            parse_event(r#"{"Type":"container","Action":"start","Actor":{"ID":"a"}}"#),
            None
        );
        assert_eq!(
            parse_event(r#"{"Type":"network","Action":"die","Actor":{"ID":"a"}}"#),
            None
        );
        assert_eq!(parse_event("not json"), None);
    }

    #[tokio::test]
    async fn a_death_reaches_the_watch_with_its_exit() {
        let exits = ContainerExits::new("false".into(), "k=v".into());
        let mut rx = exits.watch("abcdef0123");
        exits.apply(ContainerEvent::Oom {
            id: "abcdef0123".into(),
        });
        exits.apply(ContainerEvent::Died {
            id: "abcdef0123".into(),
            code: Some(137),
        });
        let exit = rx.wait_for(Option::is_some).await.unwrap().unwrap();
        assert_eq!(exit.signal, Some(9));
        assert!(exit.oom);
        // A later removal does not overwrite the exit.
        exits.apply(ContainerEvent::Removed {
            id: "abcdef0123".into(),
        });
        assert_eq!(rx.borrow().unwrap().signal, Some(9));
        // Other containers' events are ignored.
        let other = exits.watch("ffff");
        exits.apply(ContainerEvent::Died {
            id: "abcdef0123".into(),
            code: Some(1),
        });
        assert!(other.borrow().is_none());
    }
}
