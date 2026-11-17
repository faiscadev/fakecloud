//! `EcsRuntime` `task_lifecycle` family — extracted from service.rs by audit-2026-05-19.

use super::restart::RestartTracker;
use super::stopping::{self, TaskContainerHandle};
use super::*;

/// One `dependsOn[]` gate the launch loop waits on.
pub(super) struct DependencyGate<'a> {
    /// The container waiting to launch.
    pub dependent: &'a str,
    /// The started container it depends on.
    pub upstream: &'a RunningContainer,
    pub upstream_plan: &'a ContainerPlan,
    /// When the upstream was launched (its `startTimeout` counts from it).
    pub upstream_started: std::time::Instant,
    pub condition: DependsOnCondition,
}

/// How a dependency wait ended without failing the task.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum GateOutcome {
    /// The condition holds: launch the dependent.
    Met,
    /// The task is being stopped: launch nothing more.
    TaskStopping,
}

/// Restart-policy state of a running task's containers, indexed like the
/// started-containers list.
pub(super) struct RestartContext<'a> {
    pub trackers: Vec<Option<RestartTracker>>,
    plans: &'a [ResolvedContainerPlan],
    /// Whether each container publishes its own awsvpc ports.
    owns_ports: &'a [bool],
    eni_ip: Option<&'a str>,
}

impl EcsRuntime {
    /// Fill in each plan's engine-specific log driver flags and bind every
    /// daemon log driver volume to the engine's json-file log directory.
    async fn bind_engine_logging(
        &self,
        mut plans: Vec<ContainerPlan>,
    ) -> Result<Vec<ContainerPlan>, RuntimeError> {
        // Ask the engine only when this task has something that depends on it.
        let needs_engine = plans.iter().any(|p| {
            p.log_configuration.is_some()
                || p.volume_mounts.iter().any(|vm| vm.log_driver.is_some())
        });
        if !needs_engine {
            return Ok(plans);
        }
        use fakecloud_core::container_net::{docker_server_async, EngineServer};
        // An engine that didn't answer is asked once more before giving up.
        let mut server = docker_server_async(&self.cli).await;
        if server == EngineServer::Unknown {
            server = docker_server_async(&self.cli).await;
        }
        let mut log_dir: Option<String> = None;
        for plan in &mut plans {
            plan.log_argv = log_driver_argv(plan.log_configuration.as_ref(), server);
            for vm in &mut plan.volume_mounts {
                if vm.log_driver.is_none() {
                    continue;
                }
                if log_dir.is_none() {
                    log_dir = Some(self.json_file_log_dir(server).await?);
                }
                vm.source = log_dir.clone().unwrap_or_default();
            }
        }
        Ok(plans)
    }

    /// The engine directory holding one directory per container, named with
    /// the container ID: Docker's `<DockerRootDir>/containers` (each holds
    /// the container's `<id>-json.log` and rotated files), Podman's
    /// `<GraphRoot>/<driver>-containers`. The engine resolves the path on
    /// its own host, so it is right when fakecloud itself runs in a
    /// container, or the engine in a VM.
    async fn json_file_log_dir(
        &self,
        server: fakecloud_core::container_net::EngineServer,
    ) -> Result<String, RuntimeError> {
        use fakecloud_core::container_net::EngineServer;
        // Which layout the directory has depends on the engine; guessing
        // wrong mounts an empty directory the daemon reads nothing from.
        if server == EngineServer::Unknown {
            return Err(RuntimeError::ContainerStart(
                "could not determine the container engine (its server did not answer \
                 `version`), so the log driver volume's directory is unknown"
                    .into(),
            ));
        }
        let docker_server = server == EngineServer::Docker;
        let native_podman =
            !docker_server && fakecloud_core::container_net::is_podman_async(&self.cli).await;
        let format = engine_log_dir_format(native_podman);
        let out = self
            .cli_output(["info", "--format", format], CLI_CALL_TIMEOUT)
            .await
            .map_err(RuntimeError::ContainerStart)?;
        let dir = if out.status.success() {
            engine_log_dir(&String::from_utf8_lossy(&out.stdout), docker_server)
        } else {
            None
        };
        dir.ok_or_else(|| {
            RuntimeError::ContainerStart(format!(
                "could not locate the container engine's log directory for a log driver volume: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        })
    }

    /// Spawn the task asynchronously. Returns immediately after transitioning
    /// the task to `PENDING`; the background task advances it to `RUNNING`
    /// once the container is created and to `STOPPED` once the container
    /// exits.
    pub fn run_task(self: Arc<Self>, state: SharedEcsState, task_id: String, account_id: String) {
        let rt = self.clone();
        let done = self.track_task_execution(&task_id);
        tokio::spawn(async move {
            let result = rt.run_task_inner(&state, &task_id, &account_id).await;
            if let Err(err) = result {
                tracing::warn!(%err, task = %task_id, "ecs task execution failed");
                // Also surface on stderr so nextest's captured-output for a
                // failed E2E shows the reason instead of just "empty logs".
                eprintln!("[ecs] task {task_id} failed: {err}");
                // A failure past the launch loop leaves the namespace
                // holders of the task's containers behind.
                rt.remove_netns_holders(&task_id).await;
                rt.abort_execs(Some(&task_id));
                // Its ENI (precreated at RunTask) and any target it reached
                // go with it.
                let region = task_region(&state, &account_id, &task_id);
                rt.finish_teardown(&state, &account_id, &region, &task_id);
                finalize_failure(&state, &account_id, &task_id, &err.to_string());
                rt.emit_state_change(
                    &state,
                    &account_id,
                    &task_id,
                    "STOPPED",
                    Some(("TaskFailedToStart", err.to_string())),
                );
                // Persist the STOPPED/failed transition so a restart sees the
                // terminal status instead of a stale PENDING/RUNNING row.
                rt.persist_snapshot().await;
            }
            rt.task_execution_finished(&task_id, &done);
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
        // Secrets resolve in the task's region (its ARN names it).
        let region = task_region(state, account_id, task_id);
        if plans.is_empty() {
            return Err(RuntimeError::ContainerStart(
                "task has no containers".into(),
            ));
        }

        // Resolve secrets for each plan. Failures fail the whole task to
        // match real ECS's "failed to retrieve secret" behaviour — there's
        // no point starting a sidecar when the app container will fail.
        let mut resolved_plans: Vec<ResolvedContainerPlan> = Vec::with_capacity(plans.len());
        let plans = self.bind_engine_logging(plans).await?;
        let reader = super::secrets::execution_role_reader(state, account_id, task_id);
        for plan in plans {
            let mut env = plan.env.clone();
            for (name, value_from) in &plan.secrets_refs {
                env.push((
                    name.clone(),
                    self.resolve_secret(account_id, &region, value_from, reader.as_ref())?,
                ));
            }
            // The agent/metadata endpoints live on fakecloud (the host);
            // the container reaches them via the platform host alias —
            // `host.docker.internal` for docker, `host.containers.internal`
            // for podman (issue #1539).
            // The task-role credentials variable is added at launch, once we
            // know whether the container's namespace reaches the agent's
            // link-local address (see `task_creds`).
            let host_alias = &self.net.host_alias;
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
            // Rewrite ECR URIs to fakecloud's local registry. The pull is run
            // by the engine on the host, not by a sibling container, so it
            // uses the registry host Lambda uses (loopback; podman machine's
            // host alias; `FAKECLOUD_ECR_REGISTRY_HOST`), not the sibling
            // host: `host.docker.internal` does not resolve on a Linux host,
            // and Docker Desktop / OrbStack only accept the plain-HTTP
            // registry over loopback.
            let local_pull_uri = fakecloud_core::ecr_uri::translate_to_local_at(
                &rp.plan.image,
                &self.registry_host,
                self.server_port,
            );
            let pull_uri = local_pull_uri.as_deref().unwrap_or(&rp.plan.image);
            fakecloud_core::container_image::pull_image(
                &self.cli,
                self.docker_config_path().as_deref(),
                pull_uri,
                fakecloud_core::container_image::RegistryTransport::for_local_rewrite(
                    local_pull_uri.is_some(),
                ),
            )
            .await
            .map_err(RuntimeError::ImagePull)?;
            // Retag the local pull URI to the AWS URI so `docker run` finds
            // the image under the user-facing name. Digest-pinned refs
            // can't be `docker tag` targets, so we fall through and run
            // under the local URI in that case.
            let run_image = if let Some(ref local_uri) = local_pull_uri {
                if fakecloud_core::ecr_uri::is_digest_ref(&rp.plan.image) {
                    local_uri.clone()
                } else {
                    let _ = fakecloud_core::container_net::output_within(
                        self.cli_command().args(["tag", local_uri, &rp.plan.image]),
                        CLI_CALL_TIMEOUT,
                    )
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
        // Task-role containers reach credentials at the agent's link-local
        // address through a namespace holder built from this image. Without
        // it they fall back to the full URI (see `task_creds`).
        let creds_helper_image = if resolved_plans
            .iter()
            .any(|rp| super::task_creds::wants_netns_holder(&rp.plan))
        {
            match self.ensure_creds_helper_image().await {
                Ok(image) => Some(image),
                Err(err) => {
                    tracing::warn!(
                        task = %task_id,
                        error = %err,
                        "ECS task-credentials helper image unavailable; task-role containers \
                         get AWS_CONTAINER_CREDENTIALS_FULL_URI instead of the link-local \
                         169.254.170.2 endpoint"
                    );
                    None
                }
            }
        } else {
            None
        };
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
            let create = self
                .cli_output(
                    [
                        "network",
                        "create",
                        "--driver",
                        "bridge",
                        "--label",
                        &format!("fakecloud-ecs-task={}", task_id),
                        // Ownership label so the startup reaper can prune the
                        // per-task awsvpc network after an ungraceful restart,
                        // matching the container label.
                        "--label",
                        &super::fakecloud_instance_label(),
                        &network_name,
                    ],
                    CLI_CALL_TIMEOUT,
                )
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

        // The ENI the task's `awsvpc` network stands in for: a private IP from
        // the task's subnet (see `eni`), reported by DescribeTasks and
        // registered with the service's target groups.
        let eni_ip = if network_created {
            Some(self.attach_task_eni(state, account_id, &region, task_id))
        } else {
            None
        };

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
        // When each started container was launched, parallel to `started`:
        // dependency start timeouts and restart attempt periods count from it.
        let mut launched_at: Vec<std::time::Instant> = Vec::with_capacity(resolved_plans.len());
        // Whether each started container owns its published ports (an
        // awsvpc container with no namespace holder), which a restart
        // republishes.
        let mut owns_ports: Vec<bool> = Vec::with_capacity(resolved_plans.len());
        'launch: for (idx, (rp, run_image)) in
            resolved_plans.iter().zip(run_images.iter()).enumerate()
        {
            // Wait for every dependsOn[] entry on this container. Upstreams
            // declared in the same task always show up earlier in the
            // launch order thanks to topo_sort_plans, so we only ever look
            // backwards into `started`.
            for dep in &rp.plan.depends_on {
                let Some(upstream_idx) = started.iter().position(|c| c.name == dep.container_name)
                else {
                    // Upstream not in this task definition (we ignored it
                    // during topo-sort too). Skip the gate — this matches
                    // the existing "ignore unknown dependency" behaviour.
                    continue;
                };
                let Some(upstream_plan) = resolved_plans
                    .iter()
                    .map(|p| &p.plan)
                    .find(|p| p.container_name == dep.container_name)
                else {
                    continue;
                };
                let gate = DependencyGate {
                    dependent: &rp.plan.container_name,
                    upstream: &started[upstream_idx],
                    upstream_plan,
                    upstream_started: launched_at[upstream_idx],
                    condition: dep.condition,
                };
                match self
                    .wait_for_depends_on(state, account_id, task_id, gate)
                    .await
                {
                    Ok(GateOutcome::Met) => {}
                    // Stopped while waiting: launch nothing more; the
                    // stopped-during-launch path below cleans up.
                    Ok(GateOutcome::TaskStopping) => break 'launch,
                    Err(err) => {
                        self.cleanup_partial_start(&started, task_id);
                        return Err(err);
                    }
                }
            }
            let mut env = rp.env.clone();
            let mut holder: Option<String> = None;
            if rp.plan.has_task_role {
                let wants_holder = super::task_creds::wants_netns_holder(&rp.plan);
                if let Some(image) = creds_helper_image.as_deref().filter(|_| wants_holder) {
                    match self
                        .start_netns_holder(&rp.plan, task_id, network_created, image)
                        .await
                    {
                        Ok(id) => holder = Some(id),
                        Err(err) => tracing::warn!(
                            task = %task_id,
                            container = %rp.plan.container_name,
                            error = %err,
                            "could not route 169.254.170.2 into the container's network; \
                             it gets AWS_CONTAINER_CREDENTIALS_FULL_URI instead"
                        ),
                    }
                }
                // A `none`-mode container gets the agent's relative URI with
                // nothing behind it, as on ECS (it has no network).
                env.extend(super::task_creds::credentials_env(
                    task_id,
                    holder.is_some() || !wants_holder,
                    &format!("http://{}:{}", self.net.host_alias, self.server_port),
                ));
            }
            let network = match holder.as_deref() {
                Some(holder) => ContainerNetwork::Joined { holder },
                None => ContainerNetwork::Own {
                    add_host_arg: self.net.add_host_arg.as_deref(),
                    awsvpc_network_ready: network_created,
                },
            };
            let argv = build_run_argv(
                &rp.plan,
                &env,
                task_id,
                &self.net.host_alias,
                network,
                run_image,
            );
            let run_out = match self.cli_output(&argv, CONTAINER_RUN_TIMEOUT).await {
                Ok(out) => out,
                Err(e) => {
                    // A timed-out `run` may still have created the container,
                    // which isn't in `started` yet: remove it by name.
                    let _ = self
                        .cli_output(
                            ["rm", "-f", &format!("{task_id}-{}", rp.plan.container_name)],
                            CLI_CALL_TIMEOUT,
                        )
                        .await;
                    // Cleanup already-started containers on launch failure.
                    self.cleanup_partial_start(&started, task_id);
                    return Err(RuntimeError::ContainerStart(e));
                }
            };
            if !run_out.status.success() {
                let err = String::from_utf8_lossy(&run_out.stderr).to_string();
                self.cleanup_partial_start(&started, task_id);
                return Err(RuntimeError::ContainerStart(err));
            }
            let container_id = String::from_utf8_lossy(&run_out.stdout).trim().to_string();
            launched_at.push(std::time::Instant::now());
            owns_ports.push(eni_ip.is_some() && holder.is_none());
            // The ENI IP isn't routable from fakecloud: record where each
            // container port was published so ELBv2 reaches it (see `eni`).
            if let Some(ip) = eni_ip.as_deref() {
                let owner = holder.as_deref().unwrap_or(&container_id);
                self.publish_awsvpc_ports(account_id, ip, owner, &rp.plan)
                    .await;
            }
            // Append to the runtime map immediately so a StopTask landing
            // between this container and the next one in the launch loop can
            // reach it.
            self.containers
                .write()
                .entry(task_id.to_string())
                .or_default()
                .push(TaskContainerHandle::new(&rp.plan, container_id.clone()));
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
            self.begin_stopping(state, account_id, task_id);
            let handles: Vec<TaskContainerHandle> = started
                .iter()
                .zip(&resolved_plans)
                .map(|(rc, rp)| TaskContainerHandle::new(&rp.plan, rc.container_id.clone()))
                .collect();
            stopping::stop_in_waves(self.container_cli(), handles).await;
            for rc in &started {
                let _ = self
                    .cli_output(["rm", "-f", &rc.container_id], CLI_CALL_TIMEOUT)
                    .await;
            }
            self.remove_netns_holders(task_id).await;
            if network_created {
                let _ = self
                    .cli_output(["network", "rm", &network_name], CLI_CALL_TIMEOUT)
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
            self.deprovision(state, account_id, &region, task_id);
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
            self.emit_state_change(
                state,
                account_id,
                task_id,
                "STOPPED",
                Some(("UserInitiated", "Task stopped during launch".into())),
            );
            self.persist_snapshot().await;
            return Ok(());
        }

        mark_running_multi(state, account_id, task_id, &started);
        let trackers: Vec<Option<RestartTracker>> = resolved_plans
            .iter()
            .zip(&launched_at)
            .map(|(rp, at)| {
                rp.plan
                    .restart_policy
                    .clone()
                    .map(|p| RestartTracker::new(p, *at))
            })
            .collect();
        for (rc, tracker) in started.iter().zip(&trackers) {
            if tracker.is_some() {
                set_restart_count(state, account_id, task_id, &rc.name, 0);
            }
        }
        self.register_lb_targets(state, account_id, task_id);
        self.emit_state_change(state, account_id, task_id, "RUNNING", None);
        // Persist the PENDING->RUNNING transition so DescribeTasks reports
        // RUNNING after a restart without waiting on restart reconciliation.
        self.persist_snapshot().await;

        // Wait for the first essential container (or, if none are
        // essential, any container) to exit. ECS task lifetime is
        // bounded by the first essential exit, after which all remaining
        // containers are stopped. While polling we also refresh each
        // container's `healthStatus` from `docker inspect` so
        // DescribeTasks reflects HEALTHCHECK transitions in near real
        // time.
        let mut restarts = RestartContext {
            trackers,
            plans: &resolved_plans,
            owns_ports: &owns_ports,
            eni_ip: eni_ip.as_deref(),
        };
        let wait_outcome = self
            .wait_for_task_exit_with_health(state, account_id, task_id, &started, &mut restarts)
            .await?;

        // The task is stopping: ECS sets its desired status to STOPPED and
        // takes it out of its target groups before stopping the rest.
        self.begin_stopping(state, account_id, task_id);

        // Stop and reap any sidecars still running. Best-effort — failures
        // here shouldn't keep the task from transitioning to STOPPED.
        let mut final_containers = started.clone();
        let mut still_running_idx: Vec<usize> = Vec::new();
        for (i, rc) in started.iter().enumerate() {
            if Some(i) == wait_outcome.exited_index {
                final_containers[i].exit_code = Some(wait_outcome.exit_code);
                continue;
            }
            // Try to grab the exit code if the container already exited
            // on its own (non-essential exits don't stop the task), then
            // fall back to `docker stop` for stragglers.
            let inspect = self
                .cli_output(
                    ["inspect", "-f", "{{.State.ExitCode}}", &rc.container_id],
                    CLI_CALL_TIMEOUT,
                )
                .await;
            let still_running = match inspect {
                Ok(out) if out.status.success() => {
                    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
                    // `docker inspect` returns 0 for not-yet-exited
                    // containers, so we additionally check `State.Running`.
                    let running = self
                        .cli_output(
                            ["inspect", "-f", "{{.State.Running}}", &rc.container_id],
                            CLI_CALL_TIMEOUT,
                        )
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
                still_running_idx.push(i);
            }
        }
        // Stragglers stop in `dependsOn` order, independent ones together.
        let stragglers: Vec<TaskContainerHandle> = still_running_idx
            .iter()
            .map(|&i| {
                TaskContainerHandle::new(&resolved_plans[i].plan, started[i].container_id.clone())
            })
            .collect();
        stopping::stop_in_waves(self.container_cli(), stragglers).await;
        for &i in &still_running_idx {
            let wait_out = self
                .cli_output(["wait", &started[i].container_id], CLI_CALL_TIMEOUT)
                .await;
            if let Ok(out) = wait_out {
                let code: i64 = String::from_utf8_lossy(&out.stdout)
                    .trim()
                    .parse()
                    .unwrap_or(-1);
                final_containers[i].exit_code = Some(code);
            }
        }

        // Capture combined stdout+stderr from every container so the
        // introspection endpoint shows logs from sidecars too.
        let mut captured = String::new();
        for rc in &started {
            let logs_out = self
                .cli_output(["logs", &rc.container_id], CLI_CALL_TIMEOUT)
                .await
                .map_err(RuntimeError::Wait)?;
            captured.push_str(&format!("[{}] ", rc.name));
            captured.push_str(&String::from_utf8_lossy(&logs_out.stdout));
            captured.push_str(&String::from_utf8_lossy(&logs_out.stderr));
        }

        // Reap every container we own.
        for rc in &started {
            let _ = self
                .cli_output(["rm", "-f", &rc.container_id], CLI_CALL_TIMEOUT)
                .await;
        }
        // Reap task-scoped docker volumes (anonymous "Docker volumes" and
        // `dockerVolumeConfiguration` with scope=task), matching AWS, which
        // deletes them when the task stops. Host binds, EFS/FSx stubs and
        // scope=shared volumes are left in place — they outlive the task.
        // `volume rm` is best-effort and no-ops on a volume still in use.
        let mut task_volumes: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for rp in &resolved_plans {
            for vm in &rp.plan.volume_mounts {
                if vm.cleanup_on_stop {
                    task_volumes.insert(vm.source.clone());
                }
            }
        }
        for vol in &task_volumes {
            let _ = self
                .cli_output(["volume", "rm", "-f", vol], CLI_CALL_TIMEOUT)
                .await;
        }
        // The namespace holders outlive their containers; remove them before
        // the per-task network they are attached to.
        self.remove_netns_holders(task_id).await;
        // Any ExecuteCommand command ended with its container.
        self.abort_execs(Some(task_id));
        // Clean up the per-task docker network for awsvpc.
        if network_created {
            let _ = self
                .cli_output(["network", "rm", &network_name], CLI_CALL_TIMEOUT)
                .await;
        }
        self.containers.write().remove(task_id);

        // Forward logs BEFORE flipping the task to STOPPED so a client
        // that polls DescribeTasks and immediately queries
        // DescribeLogStreams can't observe the STOPPED transition before
        // the awslogs group/stream has been materialised.
        self.forward_awslogs_if_configured(state, account_id, task_id, &captured);
        let exit_code = wait_outcome.exit_code;
        // A StopTask (or scale-down) keeps its UserInitiated stop code and
        // reason, as on ECS, rather than the container exit it caused.
        let (stop_code, stopped_reason) = match user_initiated_stop(state, account_id, task_id) {
            Some(reason) => ("UserInitiated", reason),
            None => (wait_outcome.stop_code, None),
        };
        self.deprovision(state, account_id, &region, task_id);
        finalize_stopped_multi(
            state,
            account_id,
            task_id,
            &final_containers,
            exit_code,
            &captured,
            stop_code,
            stopped_reason.clone(),
        );
        self.emit_state_change(
            state,
            account_id,
            task_id,
            "STOPPED",
            Some((
                stop_code,
                stopped_reason.unwrap_or_else(|| format!("Exit code {}", exit_code)),
            )),
        );
        // Persist the terminal STOPPED transition + captured exit codes so a
        // restart reflects the completed task instead of a stale RUNNING row.
        self.persist_snapshot().await;
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
    ///
    /// A container with a restart policy that exits is restarted in place
    /// (`start` on the same container, so its ID, mounts, network namespace
    /// and published ports stay) when [`RestartTracker::decide`] allows it,
    /// and then doesn't count as exited.
    pub(super) async fn wait_for_task_exit_with_health(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        started: &[RunningContainer],
        restarts: &mut RestartContext<'_>,
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
                let inspect = self
                    .cli_output(
                        ["inspect", "-f", "{{.State.Running}}", &rc.container_id],
                        CLI_CALL_TIMEOUT,
                    )
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
                // Only reached once the container is no longer running, so
                // `wait` returns at once on a healthy daemon.
                let wait_out = self
                    .cli_output(["wait", &rc.container_id], CLI_CALL_TIMEOUT)
                    .await
                    .map_err(RuntimeError::Wait)?;
                if !wait_out.status.success() {
                    let err = String::from_utf8_lossy(&wait_out.stderr).to_string();
                    return Err(RuntimeError::Wait(err));
                }
                let parsed: Option<i64> = String::from_utf8_lossy(&wait_out.stdout)
                    .trim()
                    .parse()
                    .ok();
                if self
                    .try_restart_in_place(state, account_id, task_id, rc, i, parsed, restarts)
                    .await
                {
                    continue;
                }
                let exit_code = parsed.unwrap_or(-1);
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

    /// Restart an exited container in place if its restart policy allows
    /// it. Returns whether it is running again.
    #[allow(clippy::too_many_arguments)]
    async fn try_restart_in_place(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        rc: &RunningContainer,
        idx: usize,
        exit_code: Option<i64>,
        restarts: &mut RestartContext<'_>,
    ) -> bool {
        let Some(tracker) = restarts.trackers.get_mut(idx).and_then(|t| t.as_mut()) else {
            return false;
        };
        let stopping = task_desired_stopped(state, account_id, task_id);
        if let Err(why) = tracker.decide(exit_code, std::time::Instant::now(), stopping) {
            tracing::info!(
                task = %task_id,
                container = %rc.name,
                exit_code = ?exit_code,
                reason = ?why,
                "container exited and is not restarted under its restart policy"
            );
            return false;
        }
        let out = self
            .cli_output(["start", &rc.container_id], CONTAINER_RUN_TIMEOUT)
            .await;
        match out {
            Ok(o) if o.status.success() => {}
            Ok(o) => {
                tracing::warn!(
                    task = %task_id,
                    container = %rc.name,
                    error = %String::from_utf8_lossy(&o.stderr).trim(),
                    "restart policy: could not restart the container"
                );
                return false;
            }
            Err(e) => {
                tracing::warn!(
                    task = %task_id,
                    container = %rc.name,
                    error = %e,
                    "restart policy: could not restart the container"
                );
                return false;
            }
        }
        // A StopTask that landed after the decision found the container
        // already exited; stop what was just started so it isn't orphaned.
        if task_desired_stopped(state, account_id, task_id) {
            let grace = stop_timeout_secs(&restarts.plans[idx].plan);
            let _ = self
                .cli_output(
                    ["stop", "--time", &grace.to_string(), &rc.container_id],
                    stop_call_timeout(grace),
                )
                .await;
            return false;
        }
        tracker.record_restart(std::time::Instant::now());
        let count = tracker.restart_count;
        tracing::info!(
            task = %task_id,
            container = %rc.name,
            exit_code = ?exit_code,
            restart_count = count,
            "container restarted in place under its restart policy"
        );
        // A container that publishes its own awsvpc ports may get new
        // ephemeral host ports on start: point the ENI's targets at them.
        if restarts.owns_ports.get(idx).copied().unwrap_or(false) {
            if let Some(ip) = restarts.eni_ip {
                self.publish_awsvpc_ports(
                    account_id,
                    ip,
                    &rc.container_id,
                    &restarts.plans[idx].plan,
                )
                .await;
            }
        }
        set_restart_count(state, account_id, task_id, &rc.name, count);
        self.persist_snapshot().await;
        true
    }

    /// Block the launch of a dependent container until its upstream
    /// reaches the requested `dependsOn[].condition`, polling `docker
    /// inspect` at a small interval.
    ///
    /// As on ECS, the wait for `COMPLETE`/`SUCCESS`/`HEALTHY` gives up once
    /// the upstream has been started for its `startTimeout`; an upstream
    /// without one is waited on until the condition resolves, the task is
    /// stopped, or the condition can no longer be met (a `SUCCESS` upstream
    /// that exited non-zero, a `HEALTHY` upstream that exited).
    ///
    /// `HEALTHY` against an upstream with no healthCheck is immediately
    /// satisfied (docker reports `Health.Status` only for containers with
    /// a HEALTHCHECK, so there is nothing to wait for).
    pub(super) async fn wait_for_depends_on(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        gate: DependencyGate<'_>,
    ) -> Result<GateOutcome, RuntimeError> {
        const POLL_INTERVAL: Duration = Duration::from_millis(200);
        let condition = gate.condition;
        if matches!(condition, DependsOnCondition::Healthy)
            && gate.upstream_plan.health_check.is_none()
        {
            return Ok(GateOutcome::Met);
        }
        loop {
            if task_desired_stopped(state, account_id, task_id) {
                return Ok(GateOutcome::TaskStopping);
            }
            if let Some(st) = inspect_container_state(&self.cli, &gate.upstream.container_id).await
            {
                if condition_is_met(condition, &st) {
                    return Ok(GateOutcome::Met);
                }
                if condition_unresolvable(condition, &st) {
                    return Err(RuntimeError::ContainerStart(format!(
                        "dependency on container {} ({}) failed: upstream exited with code {}",
                        gate.upstream.name,
                        condition.as_aws_str(),
                        st.exit_code,
                    )));
                }
            }
            if restart::dependency_timed_out(
                condition,
                gate.upstream_plan.start_timeout,
                gate.upstream_started,
                std::time::Instant::now(),
            ) {
                return Err(RuntimeError::ContainerStart(
                    restart::dependency_timeout_reason(
                        gate.dependent,
                        &gate.upstream.name,
                        condition,
                        gate.upstream_plan.start_timeout.unwrap_or_default(),
                    ),
                ));
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
        let cli = self.container_cli();
        let ids: Vec<String> = started.iter().map(|c| c.container_id.clone()).collect();
        // Namespace holders go after the containers sharing them and before
        // the network they are attached to.
        let holders = self
            .netns_holders
            .write()
            .remove(task_id)
            .unwrap_or_default();
        let network = format!("fakecloud-ecs-{task_id}");
        // Drop the runtime map entry we pre-registered before the launch loop
        // so a failed launch doesn't leave a stale (now-empty) key that
        // future StopTask calls would treat as a live task.
        self.containers.write().remove(task_id);
        tokio::spawn(async move {
            for id in ids.into_iter().chain(holders) {
                let _ = cli.output(["rm", "-f", &id], CLI_CALL_TIMEOUT).await;
            }
            let _ = cli
                .output(["network", "rm", &network], CLI_CALL_TIMEOUT)
                .await;
        });
    }

    /// Kill every running container the runtime owns. Called on reset /
    /// shutdown so docker state matches fakecloud state after a fresh
    /// boot.
    pub async fn stop_all(&self) {
        self.abort_execs(None);
        if let Some(k) = &self.k8s {
            k.stop_all().await;
            return;
        }
        let ids: Vec<String> = self
            .containers
            .read()
            .values()
            .flat_map(|list| list.iter().map(|c| c.container_id.clone()))
            .collect();
        for id in ids {
            let _ = self.cli_output(["kill", &id], CLI_CALL_TIMEOUT).await;
            let _ = self.cli_output(["rm", &id], CLI_CALL_TIMEOUT).await;
        }
        self.containers.write().clear();
        let holders: Vec<String> = self
            .netns_holders
            .write()
            .drain()
            .flat_map(|(_, ids)| ids)
            .collect();
        for id in holders {
            let _ = self.cli_output(["rm", "-f", &id], CLI_CALL_TIMEOUT).await;
        }
    }
}
