//! `EcsRuntime` `task_lifecycle` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcsRuntime {
    /// Spawn the task asynchronously. Returns immediately after transitioning
    /// the task to `PENDING`; the background task advances it to `RUNNING`
    /// once the container is created and to `STOPPED` once the container
    /// exits.
    pub fn run_task(self: Arc<Self>, state: SharedEcsState, task_id: String, account_id: String) {
        let rt = self.clone();
        tokio::spawn(async move {
            if let Err(err) = rt.run_task_inner(&state, &task_id, &account_id).await {
                tracing::warn!(%err, task = %task_id, "ecs task execution failed");
                // Also surface on stderr so nextest's captured-output for a
                // failed E2E shows the reason instead of just "empty logs".
                eprintln!("[ecs] task {task_id} failed: {err}");
                finalize_failure(&state, &account_id, &task_id, &err.to_string());
                rt.emit_state_change(
                    &state,
                    &account_id,
                    &task_id,
                    "STOPPED",
                    Some(("TaskFailedToStart", err.to_string())),
                );
            }
        });
    }

    pub async fn run_task_inner(
        &self,
        state: &SharedEcsState,
        task_id: &str,
        account_id: &str,
    ) -> Result<(), RuntimeError> {
        if self.k8s.is_some() {
            return self.k8s_run_task_inner(state, task_id, account_id).await;
        }
        // Build a per-container launch plan up-front so we hold the read
        // lock once. Each entry carries everything needed to compose a
        // `docker run` invocation for one container in the task.
        let plans = build_container_plans(state, account_id, task_id, self.server_port)?;
        if plans.is_empty() {
            return Err(RuntimeError::ContainerStart(
                "task has no containers".into(),
            ));
        }

        // Resolve secrets for each plan. Failures fail the whole task to
        // match real ECS's "failed to retrieve secret" behaviour — there's
        // no point starting a sidecar when the app container will fail.
        let mut resolved_plans: Vec<ResolvedContainerPlan> = Vec::with_capacity(plans.len());
        for plan in plans {
            let mut env = plan.env.clone();
            for (name, value_from) in &plan.secrets_refs {
                match self.resolve_secret(account_id, value_from) {
                    Some(v) => env.push((name.clone(), v)),
                    None => {
                        return Err(RuntimeError::ContainerStart(format!(
                            "failed to resolve secret {name} from {value_from}"
                        )));
                    }
                }
            }
            // The agent/metadata endpoints live on fakecloud (the host);
            // the container reaches them via the platform host alias —
            // `host.docker.internal` for docker, `host.containers.internal`
            // for podman (issue #1539).
            let host_alias = &self.net.host_alias;
            if plan.has_task_role {
                env.push((
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI".into(),
                    format!(
                        "http://{host_alias}:{}/_fakecloud/ecs/creds/{}",
                        self.server_port, task_id
                    ),
                ));
            }
            env.push((
                "ECS_CONTAINER_METADATA_URI".into(),
                format!(
                    "http://{host_alias}:{}/_fakecloud/ecs/v3/{}",
                    self.server_port, task_id
                ),
            ));
            env.push((
                "ECS_CONTAINER_METADATA_URI_V4".into(),
                format!(
                    "http://{host_alias}:{}/_fakecloud/ecs/v4/{}",
                    self.server_port, task_id
                ),
            ));
            resolved_plans.push(ResolvedContainerPlan { plan, env });
        }

        // Pull every distinct image up-front so a second container's pull
        // failure surfaces before we leave the first container running.
        mark_pull_started(state, account_id, task_id);
        let mut run_images: Vec<String> = Vec::with_capacity(resolved_plans.len());
        let mut image_digests: Vec<Option<String>> = Vec::with_capacity(resolved_plans.len());
        for rp in &resolved_plans {
            // Rewrite ECR URIs to fakecloud's local registry at the sibling
            // host (`127.0.0.1` on the host, `host.docker.internal` when
            // fakecloud is containerized) so the daemon/sibling can reach
            // fakecloud's published registry port (issue #1539, bug 0.8).
            let local_pull_uri = fakecloud_core::ecr_uri::translate_to_local_at(
                &rp.plan.image,
                &self.net.sibling_host,
                self.server_port,
            );
            let pull_uri = local_pull_uri.as_deref().unwrap_or(&rp.plan.image);
            let pull_out = self
                .cli_command()
                .args(["pull", pull_uri])
                .output()
                .await
                .map_err(|e| RuntimeError::ImagePull(e.to_string()))?;
            if !pull_out.status.success() {
                let err = String::from_utf8_lossy(&pull_out.stderr).to_string();
                return Err(RuntimeError::ImagePull(err));
            }
            // Retag the local pull URI to the AWS URI so `docker run` finds
            // the image under the user-facing name. Digest-pinned refs
            // can't be `docker tag` targets, so we fall through and run
            // under the local URI in that case.
            let run_image = if let Some(ref local_uri) = local_pull_uri {
                if fakecloud_core::ecr_uri::is_digest_ref(&rp.plan.image) {
                    local_uri.clone()
                } else {
                    let _ = self
                        .cli_command()
                        .args(["tag", local_uri, &rp.plan.image])
                        .output()
                        .await;
                    rp.plan.image.clone()
                }
            } else {
                rp.plan.image.clone()
            };
            // Best-effort image digest extraction so DescribeTasks emits
            // the resolved digest the way real ECS does. Failures here
            // (e.g. CLI without RepoDigests) are silent — digest stays
            // `None` rather than failing the task.
            let digest = self.lookup_image_digest(pull_uri).await;
            run_images.push(run_image);
            image_digests.push(digest);
        }
        mark_pull_stopped(state, account_id, task_id);

        // For awsvpc network mode, create a per-task docker network so
        // containers share an isolated bridge. Clean it up when the task
        // stops. Network creation is best-effort: on failure we fall back
        // to the default bridge and continue.
        let awsvpc_network = resolved_plans
            .iter()
            .any(|rp| rp.plan.network_mode.as_deref() == Some("awsvpc"));
        let network_name = format!("fakecloud-ecs-{}", task_id);
        let network_created = if awsvpc_network {
            let create = Command::new(&self.cli)
                .args([
                    "network",
                    "create",
                    "--driver",
                    "bridge",
                    "--label",
                    &format!("fakecloud-ecs-task={}", task_id),
                    // Ownership label so the startup reaper can prune the
                    // per-task awsvpc network after an ungraceful restart,
                    // matching the container label.
                    &super::fakecloud_instance_label(),
                    &network_name,
                ])
                .output()
                .await;
            match create {
                Ok(out) if out.status.success() => {
                    tracing::info!(
                        task = %task_id,
                        network = %network_name,
                        "created awsvpc docker network"
                    );
                    true
                }
                Ok(out) => {
                    let err = String::from_utf8_lossy(&out.stderr);
                    tracing::warn!(
                        task = %task_id,
                        network = %network_name,
                        error = %err,
                        "awsvpc network creation failed; falling back to default bridge"
                    );
                    false
                }
                Err(e) => {
                    tracing::warn!(
                        task = %task_id,
                        network = %network_name,
                        error = %e,
                        "awsvpc network creation failed; falling back to default bridge"
                    );
                    false
                }
            }
        } else {
            false
        };

        if network_created {
            let eni_id = format!(
                "eni-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .replace('-', "")
                    .get(..17)
                    .unwrap_or("")
            );
            let mac = format!(
                "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                rand::random::<u8>(),
                rand::random::<u8>(),
                rand::random::<u8>(),
                rand::random::<u8>(),
                rand::random::<u8>()
            );
            let ip = format!("10.0.{}.{}", rand::random::<u8>(), rand::random::<u8>());
            let mut accounts = state.write();
            if let Some(st) = accounts.get_mut(account_id) {
                if let Some(task) = st.tasks.get_mut(task_id) {
                    task.attachments.push(crate::state::TaskAttachment {
                        id: eni_id.clone(),
                        attachment_type: "eni".into(),
                        status: "ATTACHED".into(),
                        details: vec![
                            crate::state::AttachmentDetail {
                                name: "subnetId".into(),
                                value: "subnet-fakecloud".into(),
                            },
                            crate::state::AttachmentDetail {
                                name: "privateIPv4Address".into(),
                                value: ip.clone(),
                            },
                            crate::state::AttachmentDetail {
                                name: "macAddress".into(),
                                value: mac.clone(),
                            },
                        ],
                    });
                }
            }
            tracing::info!(
                task = %task_id,
                eni = %eni_id,
                ip = %ip,
                "populated awsvpc ENI attachment"
            );
        }

        // Launch every container detached, in topological order. Before
        // each `docker run` we honour the dependent's `dependsOn[]` by
        // polling docker until each upstream container reaches the
        // requested condition (START/COMPLETE/SUCCESS/HEALTHY). If any
        // fails to start (or an upstream gate times out), kill the
        // already-started containers and bail — partial-launch state is
        // harder to reason about than a clean failure.
        // Register the task in `self.containers` BEFORE the launch loop so a
        // concurrent StopTask / scale-down / DeleteService that arrives
        // during the multi-second pull+run window can find the entry and stop
        // whatever has been launched so far. (`stop_task` is a no-op when the
        // key is absent — the orphan-on-race bug.) We append each container
        // id to this entry as it starts, mirroring the k8s backend which
        // registers the Pod name before `create_pod`.
        self.containers
            .write()
            .entry(task_id.to_string())
            .or_default();
        let mut started: Vec<RunningContainer> = Vec::with_capacity(resolved_plans.len());
        for (idx, (rp, run_image)) in resolved_plans.iter().zip(run_images.iter()).enumerate() {
            // Wait for every dependsOn[] entry on this container. Upstreams
            // declared in the same task always show up earlier in the
            // launch order thanks to topo_sort_plans, so we only ever look
            // backwards into `started`.
            for dep in &rp.plan.depends_on {
                let upstream = match started.iter().find(|c| c.name == dep.container_name) {
                    Some(u) => u,
                    // Upstream not in this task definition (we ignored it
                    // during topo-sort too). Skip the gate — this matches
                    // the existing "ignore unknown dependency" behaviour.
                    None => continue,
                };
                // Whether the upstream has a healthCheck configured —
                // governs the HEALTHY shortcut: AWS treats HEALTHY as
                // immediately satisfied when the upstream has no probe.
                let upstream_has_health_check = resolved_plans
                    .iter()
                    .find(|p| p.plan.container_name == dep.container_name)
                    .is_some_and(|p| p.plan.health_check.is_some());
                if let Err(err) = self
                    .wait_for_depends_on(upstream, dep.condition, upstream_has_health_check)
                    .await
                {
                    self.cleanup_partial_start(&started, task_id);
                    return Err(err);
                }
            }
            let argv = build_run_argv(
                &rp.plan,
                &rp.env,
                task_id,
                &self.net.host_alias,
                self.net.add_host_arg.as_deref(),
                run_image,
                network_created,
            );
            let mut cmd = Command::new(&self.cli);
            cmd.args(&argv);
            let run_out = cmd.output().await.map_err(|e| {
                // Cleanup already-started containers on launch failure.
                self.cleanup_partial_start(&started, task_id);
                RuntimeError::ContainerStart(e.to_string())
            })?;
            if !run_out.status.success() {
                let err = String::from_utf8_lossy(&run_out.stderr).to_string();
                self.cleanup_partial_start(&started, task_id);
                return Err(RuntimeError::ContainerStart(err));
            }
            let container_id = String::from_utf8_lossy(&run_out.stdout).trim().to_string();
            // Append to the runtime map immediately so a StopTask landing
            // between this container and the next one in the launch loop can
            // reach it.
            self.containers
                .write()
                .entry(task_id.to_string())
                .or_default()
                .push((rp.plan.container_name.clone(), container_id.clone()));
            started.push(RunningContainer {
                name: rp.plan.container_name.clone(),
                container_id,
                essential: rp.plan.essential,
                exit_code: None,
                network_bindings: network_bindings_for(&rp.plan),
                image_digest: image_digests.get(idx).cloned().unwrap_or(None),
            });
        }

        // A StopTask / scale-down / DeleteService that raced the launch may
        // have flipped this task's desired_status to STOPPED while we were
        // pulling/running (and may have already issued `docker stop` against
        // the containers it could see in the runtime map). If so, stop every
        // container we launched and finalize as a clean stop instead of
        // marking the task RUNNING with a live workload the user asked to
        // kill. Mirrors the k8s backend, which observes a 404 from the
        // StopTask-deleted Pod and finalizes the same way.
        if task_desired_stopped(state, account_id, task_id) {
            for rc in &started {
                let _ = Command::new(&self.cli)
                    .args(["stop", "--time", "10", &rc.container_id])
                    .output()
                    .await;
                let _ = Command::new(&self.cli)
                    .args(["rm", "-f", &rc.container_id])
                    .output()
                    .await;
            }
            if network_created {
                let _ = Command::new(&self.cli)
                    .args(["network", "rm", &network_name])
                    .output()
                    .await;
            }
            self.containers.write().remove(task_id);
            let stopped: Vec<RunningContainer> = started
                .iter()
                .map(|rc| RunningContainer {
                    exit_code: Some(rc.exit_code.unwrap_or(137)),
                    ..rc.clone()
                })
                .collect();
            finalize_stopped_multi(
                state,
                account_id,
                task_id,
                &stopped,
                137,
                "",
                "UserInitiated",
                Some("Task stopped during launch".into()),
            );
            self.deregister_lb_targets(state, account_id, task_id);
            self.emit_state_change(
                state,
                account_id,
                task_id,
                "STOPPED",
                Some(("UserInitiated", "Task stopped during launch".into())),
            );
            return Ok(());
        }

        mark_running_multi(state, account_id, task_id, &started);
        self.register_lb_targets(state, account_id, task_id);
        self.emit_state_change(state, account_id, task_id, "RUNNING", None);

        // Wait for the first essential container (or, if none are
        // essential, any container) to exit. ECS task lifetime is
        // bounded by the first essential exit, after which all remaining
        // containers are stopped. While polling we also refresh each
        // container's `healthStatus` from `docker inspect` so
        // DescribeTasks reflects HEALTHCHECK transitions in near real
        // time.
        let wait_outcome = self
            .wait_for_task_exit_with_health(state, account_id, task_id, &started)
            .await?;

        // Stop and reap any sidecars still running. Best-effort — failures
        // here shouldn't keep the task from transitioning to STOPPED.
        let mut final_containers = started.clone();
        for (i, rc) in started.iter().enumerate() {
            if Some(i) == wait_outcome.exited_index {
                final_containers[i].exit_code = Some(wait_outcome.exit_code);
                continue;
            }
            // Try to grab the exit code if the container already exited
            // on its own (non-essential exits don't stop the task), then
            // fall back to `docker stop` for stragglers.
            let inspect = Command::new(&self.cli)
                .args(["inspect", "-f", "{{.State.ExitCode}}", &rc.container_id])
                .output()
                .await;
            let still_running = match inspect {
                Ok(out) if out.status.success() => {
                    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
                    // `docker inspect` returns 0 for not-yet-exited
                    // containers, so we additionally check `State.Running`.
                    let running = Command::new(&self.cli)
                        .args(["inspect", "-f", "{{.State.Running}}", &rc.container_id])
                        .output()
                        .await
                        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
                        .unwrap_or(false);
                    if !running {
                        if let Ok(code) = s.parse::<i64>() {
                            final_containers[i].exit_code = Some(code);
                        }
                    }
                    running
                }
                _ => false,
            };
            if still_running {
                let _ = Command::new(&self.cli)
                    .args(["stop", "--time", "10", &rc.container_id])
                    .output()
                    .await;
                let wait_out = Command::new(&self.cli)
                    .args(["wait", &rc.container_id])
                    .output()
                    .await;
                if let Ok(out) = wait_out {
                    let code: i64 = String::from_utf8_lossy(&out.stdout)
                        .trim()
                        .parse()
                        .unwrap_or(-1);
                    final_containers[i].exit_code = Some(code);
                }
            }
        }

        // Capture combined stdout+stderr from every container so the
        // introspection endpoint shows logs from sidecars too.
        let mut captured = String::new();
        for rc in &started {
            let logs_out = Command::new(&self.cli)
                .args(["logs", &rc.container_id])
                .output()
                .await
                .map_err(|e| RuntimeError::Wait(e.to_string()))?;
            captured.push_str(&format!("[{}] ", rc.name));
            captured.push_str(&String::from_utf8_lossy(&logs_out.stdout));
            captured.push_str(&String::from_utf8_lossy(&logs_out.stderr));
        }

        // Reap every container we own.
        for rc in &started {
            let _ = Command::new(&self.cli)
                .args(["rm", "-f", &rc.container_id])
                .output()
                .await;
        }
        // Clean up the per-task docker network for awsvpc.
        if network_created {
            let _ = Command::new(&self.cli)
                .args(["network", "rm", &network_name])
                .output()
                .await;
        }
        self.containers.write().remove(task_id);

        // Forward logs BEFORE flipping the task to STOPPED so a client
        // that polls DescribeTasks and immediately queries
        // DescribeLogStreams can't observe the STOPPED transition before
        // the awslogs group/stream has been materialised.
        self.forward_awslogs_if_configured(state, account_id, task_id, &captured);
        let exit_code = wait_outcome.exit_code;
        finalize_stopped_multi(
            state,
            account_id,
            task_id,
            &final_containers,
            exit_code,
            &captured,
            wait_outcome.stop_code,
            None,
        );
        self.deregister_lb_targets(state, account_id, task_id);
        self.emit_state_change(
            state,
            account_id,
            task_id,
            "STOPPED",
            Some((wait_outcome.stop_code, format!("Exit code {}", exit_code))),
        );
        Ok(())
    }

    /// Wait for the task to reach a stop condition (any essential
    /// container exits, or every container exits when none are
    /// essential) while also polling `docker inspect .State.Health.Status`
    /// on every iteration to push the latest `healthStatus` onto each
    /// task container — so DescribeTasks shows live HEALTHCHECK
    /// transitions instead of the boot-time `UNKNOWN`. Returns the
    /// index into `started` of the container whose exit determined the
    /// task lifetime, its exit code, and the stopCode.
    pub(super) async fn wait_for_task_exit_with_health(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        started: &[RunningContainer],
    ) -> Result<TaskExitOutcome, RuntimeError> {
        let any_essential = started.iter().any(|c| c.essential);
        let mut working: Vec<RunningContainer> = started.to_vec();
        let mut first_exited: Option<usize> = None;
        loop {
            // Refresh health status before checking exits so a container
            // that goes UNHEALTHY -> exits in the same iteration leaves
            // its final health state on the task before we transition to
            // STOPPED.
            self.refresh_health_status(state, account_id, task_id, started)
                .await;
            for (i, rc) in started.iter().enumerate() {
                if working[i].exit_code.is_some() {
                    continue;
                }
                let inspect = Command::new(&self.cli)
                    .args(["inspect", "-f", "{{.State.Running}}", &rc.container_id])
                    .output()
                    .await;
                let running = match inspect {
                    Ok(out) if out.status.success() => {
                        String::from_utf8_lossy(&out.stdout).trim() == "true"
                    }
                    _ => false,
                };
                if running {
                    continue;
                }
                let wait_out = Command::new(&self.cli)
                    .args(["wait", &rc.container_id])
                    .output()
                    .await
                    .map_err(|e| RuntimeError::Wait(e.to_string()))?;
                if !wait_out.status.success() {
                    let err = String::from_utf8_lossy(&wait_out.stderr).to_string();
                    return Err(RuntimeError::Wait(err));
                }
                let exit_code: i64 = String::from_utf8_lossy(&wait_out.stdout)
                    .trim()
                    .parse()
                    .unwrap_or(-1);
                working[i].exit_code = Some(exit_code);
                if first_exited.is_none() && (rc.essential || !any_essential) {
                    first_exited = Some(i);
                }
            }
            if task_should_stop(&working) {
                let idx = first_exited
                    .or_else(|| working.iter().position(|c| c.exit_code.is_some()))
                    .unwrap_or(0);
                let exit_code = working[idx].exit_code.unwrap_or(-1);
                return Ok(TaskExitOutcome {
                    exited_index: Some(idx),
                    exit_code,
                    stop_code: if any_essential {
                        "EssentialContainerExited"
                    } else {
                        "TaskCompleted"
                    },
                });
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Block the launch of a dependent container until its upstream
    /// reaches the requested `dependsOn[].condition`. We poll
    /// `docker inspect` at a small interval; the wait is bounded by an
    /// AWS-style timeout (120s by default — long enough for image
    /// startup but short enough to surface bugs as a clean
    /// `ContainerStart` failure).
    ///
    /// `upstream_has_health_check` is needed for the `HEALTHY` branch:
    /// when the upstream has no healthCheck, AWS treats `HEALTHY` as
    /// immediately satisfied (otherwise the dependent would block
    /// forever, since docker reports `Health.Status` only when the
    /// container has a HEALTHCHECK directive).
    pub(super) async fn wait_for_depends_on(
        &self,
        upstream: &RunningContainer,
        condition: DependsOnCondition,
        upstream_has_health_check: bool,
    ) -> Result<(), RuntimeError> {
        // Bounded wait — chosen to comfortably cover slow init scripts
        // without letting a wedged dependency stall a task indefinitely.
        const WAIT_TIMEOUT: Duration = Duration::from_secs(120);
        const POLL_INTERVAL: Duration = Duration::from_millis(200);

        // HEALTHY against an upstream without a healthCheck: AWS treats
        // this as immediately satisfied because there's no probe to
        // observe. Skip the polling loop entirely so the dependent isn't
        // wedged forever waiting for a status that docker will never set.
        if matches!(condition, DependsOnCondition::Healthy) && !upstream_has_health_check {
            return Ok(());
        }

        let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            let inspect = inspect_container_state(&self.cli, &upstream.container_id).await;
            if let Some(state) = inspect {
                if condition_is_met(condition, &state) {
                    return Ok(());
                }
                // SUCCESS specifically: if the container exited with a
                // non-zero code, the gate can never be satisfied. Bail
                // immediately rather than waiting for the timeout — this
                // matches ECS's "stoppedReason: dependency failed" path.
                if matches!(condition, DependsOnCondition::Success)
                    && state.exited
                    && state.exit_code != 0
                {
                    return Err(RuntimeError::ContainerStart(format!(
                        "dependency on container {} ({}) failed: upstream exited with code {}",
                        upstream.name,
                        DependsOnCondition::Success.as_aws_str(),
                        state.exit_code,
                    )));
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(RuntimeError::ContainerStart(format!(
                    "timed out waiting for container {} to reach condition {}",
                    upstream.name,
                    condition.as_aws_str(),
                )));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Best-effort cleanup of containers we already started when a later
    /// container in the task failed to launch. Without this, half-launched
    /// tasks leak docker containers. `task_id` mirrors the value used at
    /// network creation so `network rm` targets the right name —
    /// deriving it from a container_id prefix was wrong (container ids
    /// are docker-assigned, not task-shaped).
    pub(super) fn cleanup_partial_start(&self, started: &[RunningContainer], task_id: &str) {
        let cli = self.cli.clone();
        let ids: Vec<String> = started.iter().map(|c| c.container_id.clone()).collect();
        let network = format!("fakecloud-ecs-{task_id}");
        // Drop the runtime map entry we pre-registered before the launch loop
        // so a failed launch doesn't leave a stale (now-empty) key that
        // future StopTask calls would treat as a live task.
        self.containers.write().remove(task_id);
        tokio::spawn(async move {
            for id in ids {
                let _ = Command::new(&cli).args(["rm", "-f", &id]).output().await;
            }
            let _ = Command::new(&cli)
                .args(["network", "rm", &network])
                .output()
                .await;
        });
    }

    /// Kill every container behind a task with the configured stop
    /// timeout. Returns true if at least one container was killed. Called
    /// synchronously from `StopTask`; the wait loop in `run_task_inner`
    /// observes the exits and transitions the task to `STOPPED`.
    pub async fn stop_task(&self, task_id: &str, reason: &str) -> bool {
        if let Some(k) = &self.k8s {
            tracing::info!(task = %task_id, reason = %reason, "ecs task stop requested (k8s)");
            return k.stop_task(task_id).await;
        }
        let containers = self.containers.read().get(task_id).cloned();
        let Some(list) = containers else {
            return false;
        };
        if list.is_empty() {
            return false;
        }
        // `docker stop` sends SIGTERM then SIGKILL after a timeout.
        for (_name, id) in &list {
            let _ = Command::new(&self.cli)
                .args(["stop", "--time", "10", id])
                .output()
                .await;
        }
        tracing::info!(task = %task_id, reason = %reason, "ecs task stop requested");
        true
    }

    /// Kill every running container the runtime owns. Called on reset /
    /// shutdown so docker state matches fakecloud state after a fresh
    /// boot.
    pub async fn stop_all(&self) {
        if let Some(k) = &self.k8s {
            k.stop_all().await;
            return;
        }
        let ids: Vec<String> = self
            .containers
            .read()
            .values()
            .flat_map(|list| list.iter().map(|(_, id)| id.clone()))
            .collect();
        for id in ids {
            let _ = Command::new(&self.cli).args(["kill", &id]).output().await;
            let _ = Command::new(&self.cli).args(["rm", &id]).output().await;
        }
        self.containers.write().clear();
    }
}
