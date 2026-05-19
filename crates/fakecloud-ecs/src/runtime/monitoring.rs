//! `EcsRuntime` `monitoring` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcsRuntime {
    /// Inspect each running container's `.State.Health.Status` and push
    /// the mapped ECS healthStatus onto the task's container list.
    /// Best-effort: a failed inspect (e.g. container already removed)
    /// leaves the previous status untouched.
    pub(super) async fn refresh_health_status(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        started: &[RunningContainer],
    ) {
        let mut updates: Vec<(String, String)> = Vec::with_capacity(started.len());
        for rc in started {
            let out = Command::new(&self.cli)
                .args([
                    "inspect",
                    "-f",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{end}}",
                    &rc.container_id,
                ])
                .output()
                .await;
            let status = match out {
                Ok(o) if o.status.success() => {
                    let raw = String::from_utf8_lossy(&o.stdout).trim().to_string();
                    if raw.is_empty() {
                        // No HEALTHCHECK on this container — leave the
                        // ECS-side status as UNKNOWN (matches AWS).
                        "UNKNOWN".to_string()
                    } else {
                        docker_health_to_ecs(&raw).to_string()
                    }
                }
                _ => continue,
            };
            updates.push((rc.name.clone(), status));
        }
        if updates.is_empty() {
            return;
        }
        let mut accounts = state.write();
        let Some(s) = accounts.get_mut(account_id) else {
            return;
        };
        let Some(task) = s.tasks.get_mut(task_id) else {
            return;
        };
        for (name, status) in updates {
            if let Some(c) = task.containers.iter_mut().find(|c| c.name == name) {
                c.health_status = Some(status);
            }
        }
    }

    /// Best-effort image digest lookup via `docker image inspect` after a
    /// pull. Returns the first `RepoDigests[0]` entry's `sha256:...` tail
    /// when present, matching what AWS ECS returns on `DescribeTasks`.
    /// `None` on any failure so digest extraction never fails the task.
    pub(super) async fn lookup_image_digest(&self, pull_uri: &str) -> Option<String> {
        let out = self
            .cli_command()
            .args([
                "image",
                "inspect",
                "-f",
                "{{index .RepoDigests 0}}",
                pull_uri,
            ])
            .output()
            .await
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if raw.is_empty() || raw == "<no value>" {
            return None;
        }
        // RepoDigests entries are `<repo>@sha256:<hex>`. Real ECS surfaces
        // the digest portion only.
        Some(
            raw.rsplit_once('@')
                .map(|(_, d)| d.to_string())
                .unwrap_or(raw),
        )
    }

    /// Emit an `ECS Task State Change` EventBridge event. No-op when no
    /// delivery bus is wired. Matches AWS event shape so downstream
    /// rules can filter on `detail.lastStatus`, `detail.stopCode`, etc.
    pub(super) fn emit_state_change(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        last_status: &str,
        stop: Option<(&str, String)>,
    ) {
        let Some(ref bus) = self.delivery_bus else {
            return;
        };
        let Some(task_view) = snapshot_task(state, account_id, task_id) else {
            return;
        };
        let mut detail = serde_json::json!({
            "taskArn": task_view.task_arn,
            "clusterArn": task_view.cluster_arn,
            "lastStatus": last_status,
            "desiredStatus": if last_status == "STOPPED" { "STOPPED" } else { "RUNNING" },
            "launchType": task_view.launch_type,
            "group": task_view.group,
            "taskDefinitionArn": task_view.task_definition_arn,
            "containers": task_view.containers,
        });
        if let Some((code, reason)) = stop {
            detail["stopCode"] = code.into();
            detail["stoppedReason"] = reason.into();
        }
        bus.put_event_to_eventbridge(
            "aws.ecs",
            "ECS Task State Change",
            &detail.to_string(),
            "default",
        );
    }

    /// Forward captured stdout/stderr to CloudWatch Logs when the task's
    /// container definition declares the `awslogs` log driver. No-op when
    /// logs_state isn't wired or the task has no awslogs config.
    pub(super) fn forward_awslogs_if_configured(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        captured: &str,
    ) {
        let Some(ref logs) = self.logs_state else {
            return;
        };
        // Clone out of the read guard so we don't hold it across the logs
        // state write.
        let (cfg, task_region) = {
            let accounts = state.read();
            let Some(s) = accounts.get(account_id) else {
                return;
            };
            let Some(task) = s.tasks.get(task_id) else {
                return;
            };
            let Some(ref cfg) = task.awslogs else {
                return;
            };
            (cfg.clone(), s.region.clone())
        };
        if captured.is_empty() {
            return;
        }
        let now = Utc::now().timestamp_millis();
        let stream_name = cfg.stream_name(task_id);
        let events: Vec<IngestEvent> = captured
            .lines()
            .enumerate()
            .map(|(i, line)| IngestEvent {
                // Stagger within the same millisecond so CloudWatch's
                // chronological-order invariant holds without relying on
                // the host clock's resolution.
                timestamp_ms: now.saturating_add(i as i64),
                message: line.to_string(),
            })
            .collect();
        append_events(
            logs,
            account_id,
            &task_region,
            &cfg.group,
            &stream_name,
            &events,
        );
    }
}
