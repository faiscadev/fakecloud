//! Kubernetes backend for ECS task execution.
//!
//! Maps an ECS task to a single native Pod: one Pod container per
//! `containerDefinitions` entry, all sharing the Pod network namespace
//! (`localhost`) — which is exactly the `awsvpc` model. The task-definition
//! parsing (`build_container_plans`) and the shared state-transition
//! helpers are reused from the Docker backend; only the "plans -> Pod"
//! mapping and the Pod-status-driven lifecycle live here.
//!
//! Mapping notes:
//! - A container that is the target of a `dependsOn` `COMPLETE`/`SUCCESS`
//!   condition becomes an **initContainer** (Kubernetes runs initContainers
//!   to completion, in order, before the app containers) — the natural fit
//!   for run-once migration/bootstrap containers. `START`/`HEALTHY`
//!   ordering among the long-running app containers isn't strictly
//!   enforceable inside one Pod (the kubelet starts them together); the
//!   `healthCheck` still becomes a readinessProbe.
//! - `healthCheck` -> container `readinessProbe` (exec).
//! - Secrets resolve exactly as on the Docker backend and are injected as
//!   env; the task-role/metadata endpoints are reached at the in-cluster
//!   `FAKECLOUD_K8S_SELF_URL`.
//! - Low-level Docker-runtime knobs (ulimits, devices, sysctls, tmpfs,
//!   capabilities) aren't translated to the Pod; `privileged`,
//!   `readonlyRootFilesystem`, and a numeric `user` are.
//!
//! ECS tasks don't require fakecloud to reach the Pod, so this works the
//! same whether fakecloud is in-cluster or not — though images must be
//! pullable by the cluster.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EmptyDirVolumeSource, EnvVar, ExecAction, LocalObjectReference, Pod,
    PodSpec, Probe, SecurityContext, Volume, VolumeMount as K8sVolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use parking_lot::RwLock;

use fakecloud_k8s::{labels, names, K8sClient, K8sEnv};

use super::{
    build_container_plans, finalize_stopped_multi, mark_pull_started, mark_pull_stopped,
    mark_running_multi, task_should_stop, ContainerPlan, DependsOnCondition, EcsRuntime,
    HealthCheckSpec, RunningContainer, RuntimeError,
};
use crate::state::SharedEcsState;

const SERVICE: &str = "ecs";
const POD_PREFIX: &str = "fakecloud-ecs";

/// Error initializing the Kubernetes backend at startup.
#[derive(Debug, thiserror::Error)]
pub enum BackendInitError {
    #[error(transparent)]
    Env(#[from] fakecloud_k8s::K8sEnvError),
    #[error("failed to connect to the Kubernetes cluster: {0}")]
    Connect(String),
}

pub(super) struct K8sTaskBackend {
    client: K8sClient,
    /// In-cluster fakecloud base URL — task-role / metadata endpoints.
    self_url: String,
    ecr_host: String,
    ecr_port: u16,
    pull_secret: Option<String>,
    /// task_id -> Pod name, so StopTask/stop_all can find the Pod.
    pods: RwLock<HashMap<String, String>>,
}

impl std::fmt::Debug for K8sTaskBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sTaskBackend")
            .field("namespace", &self.client.namespace())
            .field("self_url", &self.self_url)
            .finish_non_exhaustive()
    }
}

impl K8sTaskBackend {
    pub(super) async fn from_env(server_port: u16) -> Result<Self, BackendInitError> {
        let env = K8sEnv::from_env(server_port)?;
        let client = K8sClient::connect(env.namespace.clone())
            .await
            .map_err(|e| BackendInitError::Connect(e.to_string()))?;
        tracing::info!(
            namespace = %env.namespace,
            self_url = %env.self_url,
            "K8s ECS backend initialized"
        );
        Ok(Self {
            client,
            self_url: env.self_url,
            ecr_host: env.ecr_host,
            ecr_port: env.ecr_port,
            pull_secret: env.pull_secret,
            pods: RwLock::new(HashMap::new()),
        })
    }

    pub(super) async fn reap_stale(&self) {
        self.client.reap_stale(SERVICE).await;
    }

    pub(super) async fn stop_task(&self, task_id: &str) -> bool {
        let pod = self.pods.read().get(task_id).cloned();
        match pod {
            Some(name) => {
                self.client.delete_pod(&name).await;
                true
            }
            None => false,
        }
    }

    pub(super) async fn stop_all(&self) {
        let names: Vec<String> = self.pods.read().values().cloned().collect();
        for name in names {
            self.client.delete_pod(&name).await;
        }
        self.pods.write().clear();
    }
}

impl EcsRuntime {
    /// Kubernetes equivalent of `run_task_inner`: build one Pod for the
    /// task, create it, drive the task state machine off Pod container
    /// statuses, capture logs, and tear the Pod down.
    pub(super) async fn k8s_run_task_inner(
        &self,
        state: &SharedEcsState,
        task_id: &str,
        account_id: &str,
    ) -> Result<(), RuntimeError> {
        let backend = self
            .k8s
            .as_ref()
            .expect("k8s_run_task_inner called without k8s backend");

        let plans = build_container_plans(state, account_id, task_id, self.server_port)?;
        if plans.is_empty() {
            return Err(RuntimeError::ContainerStart(
                "task has no containers".into(),
            ));
        }

        // Resolve secrets + inject the task-role / metadata endpoints
        // (reached in-cluster at self_url), mirroring the Docker path.
        let mut resolved: Vec<(ContainerPlan, Vec<(String, String)>)> =
            Vec::with_capacity(plans.len());
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
            let base = backend.self_url.trim_end_matches('/');
            if plan.has_task_role {
                env.push((
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI".into(),
                    format!("{base}/_fakecloud/ecs/creds/{task_id}"),
                ));
            }
            env.push((
                "ECS_CONTAINER_METADATA_URI".into(),
                format!("{base}/_fakecloud/ecs/v3/{task_id}"),
            ));
            env.push((
                "ECS_CONTAINER_METADATA_URI_V4".into(),
                format!("{base}/_fakecloud/ecs/v4/{task_id}"),
            ));
            resolved.push((plan, env));
        }

        let pod_name = names::pod_name(POD_PREFIX, task_id, task_id);
        let (pod, container_map) = build_task_pod(
            &pod_name,
            backend.client.namespace(),
            backend.client.instance_id(),
            backend.pull_secret.as_deref(),
            &backend.ecr_host,
            backend.ecr_port,
            task_id,
            &resolved,
        );

        backend
            .pods
            .write()
            .insert(task_id.to_string(), pod_name.clone());

        mark_pull_started(state, account_id, task_id);
        let create = backend.client.create_pod(&pod).await;
        mark_pull_stopped(state, account_id, task_id);
        if let Err(e) = create {
            backend.pods.write().remove(task_id);
            return Err(RuntimeError::ContainerStart(format!(
                "create task pod: {e}"
            )));
        }

        // Wait until the app containers have started (or the Pod already
        // reached a terminal phase for a fast run-once task), then mark
        // RUNNING with the container runtime ids.
        let api = backend.client.pods();
        let started = build_running_list(&container_map);
        let mut marked_running = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(300);
        loop {
            let pod = match api.get(&pod_name).await {
                Ok(p) => p,
                // A 404 means the Pod is gone — almost always an
                // intentional StopTask (which deletes the Pod). Finalize
                // as a clean stop instead of surfacing TaskFailedToStart.
                Err(e) if is_not_found(&e) => {
                    if !marked_running {
                        mark_running_multi(state, account_id, task_id, &started);
                        self.emit_state_change(state, account_id, task_id, "RUNNING", None);
                    }
                    return self
                        .k8s_finalize(
                            state,
                            account_id,
                            task_id,
                            &pod_name,
                            &container_map,
                            started.clone(),
                            true,
                        )
                        .await;
                }
                Err(e) => return Err(RuntimeError::Wait(format!("get pod {pod_name}: {e}"))),
            };
            let phase = pod
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Pending");
            let exits = container_exit_codes(&pod, &container_map);

            if !marked_running && (phase == "Running" || phase == "Succeeded" || phase == "Failed")
            {
                mark_running_multi(state, account_id, task_id, &started);
                self.register_lb_targets(state, account_id, task_id);
                self.emit_state_change(state, account_id, task_id, "RUNNING", None);
                marked_running = true;
            }

            // Determine task-stop from current container exit codes.
            let mut snapshot = started.clone();
            for rc in snapshot.iter_mut() {
                rc.exit_code = exits.get(&rc.name).copied().flatten();
            }
            let terminal = phase == "Succeeded" || phase == "Failed";
            if terminal || task_should_stop(&snapshot) {
                return self
                    .k8s_finalize(
                        state,
                        account_id,
                        task_id,
                        &pod_name,
                        &container_map,
                        snapshot,
                        false,
                    )
                    .await;
            }
            if std::time::Instant::now() >= deadline && !marked_running {
                backend.client.delete_pod(&pod_name).await;
                backend.pods.write().remove(task_id);
                return Err(RuntimeError::ContainerStart(format!(
                    "task pod {pod_name} did not start within 300s"
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn k8s_finalize(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        pod_name: &str,
        container_map: &[ContainerMapEntry],
        mut final_containers: Vec<RunningContainer>,
        stopped_externally: bool,
    ) -> Result<(), RuntimeError> {
        let backend = self.k8s.as_ref().expect("k8s backend");

        let (primary, stop_code) = task_outcome(&final_containers, stopped_externally);

        // Capture logs from every container (init + app).
        let mut captured = String::new();
        for entry in container_map {
            let logs = backend
                .client
                .pod_logs(pod_name, Some(&entry.k8s_name))
                .await
                .unwrap_or_default();
            captured.push_str(&format!("[{}] ", entry.original));
            captured.push_str(&logs);
        }

        backend.client.delete_pod(pod_name).await;
        backend.pods.write().remove(task_id);

        // Fill any still-unknown exit codes with the primary so the task
        // record is complete.
        for rc in final_containers.iter_mut() {
            if rc.exit_code.is_none() {
                rc.exit_code = Some(primary);
            }
        }

        // A StopTask sets stopCode=UserInitiated + a reason before the
        // runtime observes the Pod deletion. Preserve that user-initiated
        // stop instead of overwriting it with EssentialContainerExited.
        let (final_stop_code, final_reason): (&str, Option<String>) = if stopped_externally {
            let existing = {
                let accounts = state.read();
                accounts
                    .get(account_id)
                    .and_then(|s| s.tasks.get(task_id))
                    .map(|t| (t.stop_code.clone(), t.stopped_reason.clone()))
            };
            match existing {
                Some((Some(sc), reason)) if sc == "UserInitiated" => ("UserInitiated", reason),
                _ => (stop_code, None),
            }
        } else {
            (stop_code, None)
        };

        self.forward_awslogs_if_configured(state, account_id, task_id, &captured);
        finalize_stopped_multi(
            state,
            account_id,
            task_id,
            &final_containers,
            primary,
            &captured,
            final_stop_code,
            final_reason.clone(),
        );
        self.deregister_lb_targets(state, account_id, task_id);
        let reason_msg = final_reason.unwrap_or_else(|| format!("Exit code {primary}"));
        self.emit_state_change(
            state,
            account_id,
            task_id,
            "STOPPED",
            Some((final_stop_code, reason_msg)),
        );
        Ok(())
    }
}

/// Decide a task's primary exit code + ECS stopCode from its containers'
/// final exit codes. Handles the case where a non-essential dependency /
/// initContainer fails before the essential container ever runs (which
/// must surface as a failure, not a spurious exit 0).
fn task_outcome(containers: &[RunningContainer], stopped_externally: bool) -> (i64, &'static str) {
    let any_essential = containers.iter().any(|c| c.essential);
    let essential_exit = containers
        .iter()
        .find(|c| c.essential && c.exit_code.is_some())
        .and_then(|c| c.exit_code);
    let failed_exit = containers
        .iter()
        .find(|c| matches!(c.exit_code, Some(code) if code != 0))
        .and_then(|c| c.exit_code);
    if stopped_externally {
        // Pod deleted out from under us (StopTask): containers were
        // terminated. 137 = SIGKILL, matching a killed container.
        (
            essential_exit.or(failed_exit).unwrap_or(137),
            "EssentialContainerExited",
        )
    } else if let Some(code) = essential_exit {
        (code, "EssentialContainerExited")
    } else if let Some(code) = failed_exit {
        // A dependency/init container failed before the essential
        // container started — report the failure, not a spurious 0.
        (code, "TaskFailedToStart")
    } else if !any_essential {
        (
            containers.iter().find_map(|c| c.exit_code).unwrap_or(0),
            "TaskCompleted",
        )
    } else {
        (0, "EssentialContainerExited")
    }
}

/// Whether a kube error is a `404 Not Found` (the Pod is gone).
fn is_not_found(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(api_err) if api_err.code == 404)
}

/// Maps a task's containers between their ECS name and the sanitized
/// (DNS-1123) Pod container name.
pub(super) struct ContainerMapEntry {
    original: String,
    k8s_name: String,
    essential: bool,
}

fn build_running_list(map: &[ContainerMapEntry]) -> Vec<RunningContainer> {
    map.iter()
        .map(|e| RunningContainer {
            name: e.original.clone(),
            container_id: e.k8s_name.clone(),
            essential: e.essential,
            exit_code: None,
            network_bindings: Vec::new(),
            image_digest: None,
        })
        .collect()
}

/// Read each app container's terminated exit code (if any) from Pod
/// status, keyed by the *original* ECS container name.
fn container_exit_codes(pod: &Pod, map: &[ContainerMapEntry]) -> HashMap<String, Option<i64>> {
    let mut by_k8s: HashMap<&str, Option<i64>> = HashMap::new();
    if let Some(status) = pod.status.as_ref() {
        for cs in status
            .container_statuses
            .iter()
            .flatten()
            .chain(status.init_container_statuses.iter().flatten())
        {
            let code = cs
                .state
                .as_ref()
                .and_then(|s| s.terminated.as_ref())
                .map(|t| t.exit_code as i64);
            by_k8s.insert(cs.name.as_str(), code);
        }
    }
    map.iter()
        .map(|e| {
            (
                e.original.clone(),
                by_k8s.get(e.k8s_name.as_str()).copied().flatten(),
            )
        })
        .collect()
}

/// Build the Pod for a task. Returns the Pod and the container-name map.
#[allow(clippy::too_many_arguments)]
fn build_task_pod(
    pod_name: &str,
    namespace: &str,
    instance_id: &str,
    pull_secret: Option<&str>,
    ecr_host: &str,
    ecr_port: u16,
    task_id: &str,
    resolved: &[(ContainerPlan, Vec<(String, String)>)],
) -> (Pod, Vec<ContainerMapEntry>) {
    // A container depended on with COMPLETE/SUCCESS runs to completion
    // first -> initContainer.
    let mut init_targets: HashSet<&str> = HashSet::new();
    for (plan, _) in resolved {
        for dep in &plan.depends_on {
            if matches!(
                dep.condition,
                DependsOnCondition::Complete | DependsOnCondition::Success
            ) {
                init_targets.insert(dep.container_name.as_str());
            }
        }
    }

    let mut used_names: HashSet<String> = HashSet::new();
    let mut map: Vec<ContainerMapEntry> = Vec::with_capacity(resolved.len());
    let mut volumes: HashMap<String, ()> = HashMap::new();
    let mut init_containers: Vec<Container> = Vec::new();
    let mut app_containers: Vec<Container> = Vec::new();

    for (idx, (plan, env)) in resolved.iter().enumerate() {
        let k8s_name = unique_dns_name(&plan.container_name, idx, &mut used_names);
        map.push(ContainerMapEntry {
            original: plan.container_name.clone(),
            k8s_name: k8s_name.clone(),
            essential: plan.essential,
        });
        let container = build_container(&k8s_name, plan, env, ecr_host, ecr_port, &mut volumes);
        if init_targets.contains(plan.container_name.as_str()) {
            init_containers.push(container);
        } else {
            app_containers.push(container);
        }
    }

    let mut pod_labels = std::collections::BTreeMap::new();
    pod_labels.insert(
        labels::MANAGED_BY.to_string(),
        labels::MANAGED_BY_VALUE.to_string(),
    );
    pod_labels.insert(labels::INSTANCE.to_string(), instance_id.to_string());
    pod_labels.insert(labels::SERVICE.to_string(), SERVICE.to_string());
    pod_labels.insert("fakecloud-ecs-task".to_string(), names::label_safe(task_id));

    let pod_volumes: Vec<Volume> = volumes
        .keys()
        .map(|name| Volume {
            name: name.clone(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        })
        .collect();

    let pull_secrets = pull_secret.map(|name| {
        vec![LocalObjectReference {
            name: name.to_string(),
        }]
    });

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(pod_labels),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".to_string()),
            init_containers: (!init_containers.is_empty()).then_some(init_containers),
            containers: app_containers,
            volumes: (!pod_volumes.is_empty()).then_some(pod_volumes),
            image_pull_secrets: pull_secrets,
            ..PodSpec::default()
        }),
        ..Pod::default()
    };
    (pod, map)
}

fn build_container(
    k8s_name: &str,
    plan: &ContainerPlan,
    env: &[(String, String)],
    ecr_host: &str,
    ecr_port: u16,
    volumes: &mut HashMap<String, ()>,
) -> Container {
    // ECR URIs resolve to the in-cluster fakecloud registry.
    let image = fakecloud_core::ecr_uri::translate_to_local_at(&plan.image, ecr_host, ecr_port)
        .unwrap_or_else(|| plan.image.clone());

    let command = (!plan.entry_point.is_empty()).then(|| plan.entry_point.clone());
    let args = (!plan.command.is_empty()).then(|| plan.command.clone());

    let env_vars: Vec<EnvVar> = env
        .iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            value_from: None,
        })
        .collect();

    let ports: Vec<ContainerPort> = plan
        .port_mappings
        .iter()
        .map(|p| ContainerPort {
            container_port: i32::from(p.container_port),
            protocol: Some(if p.protocol.eq_ignore_ascii_case("udp") {
                "UDP".to_string()
            } else {
                "TCP".to_string()
            }),
            ..ContainerPort::default()
        })
        .collect();

    // Volume mounts -> one emptyDir per distinct source, shared within the
    // Pod (task-scoped scratch). Host binds/EFS/FSx degrade to pod-local
    // emptyDir on k8s (no host path on the node).
    let volume_mounts: Vec<K8sVolumeMount> = plan
        .volume_mounts
        .iter()
        .map(|vm| {
            let vol_name = volume_name_for(&vm.source);
            volumes.entry(vol_name.clone()).or_insert(());
            K8sVolumeMount {
                name: vol_name,
                mount_path: vm.container_path.clone(),
                read_only: Some(vm.read_only),
                ..K8sVolumeMount::default()
            }
        })
        .collect();

    let privileged = plan
        .linux_parameters
        .as_ref()
        .map(|lp| lp.privileged)
        .unwrap_or(false);
    let security_context = if privileged
        || plan.readonly_rootfs
        || plan
            .user
            .as_deref()
            .and_then(|u| u.parse::<i64>().ok())
            .is_some()
    {
        Some(SecurityContext {
            privileged: privileged.then_some(true),
            read_only_root_filesystem: plan.readonly_rootfs.then_some(true),
            run_as_user: plan.user.as_deref().and_then(|u| u.parse::<i64>().ok()),
            ..SecurityContext::default()
        })
    } else {
        None
    };

    Container {
        name: k8s_name.to_string(),
        image: Some(image),
        command,
        args,
        env: (!env_vars.is_empty()).then_some(env_vars),
        ports: (!ports.is_empty()).then_some(ports),
        volume_mounts: (!volume_mounts.is_empty()).then_some(volume_mounts),
        working_dir: plan.working_directory.clone(),
        readiness_probe: plan.health_check.as_ref().and_then(health_probe),
        security_context,
        ..Container::default()
    }
}

/// Translate an ECS `healthCheck` into a k8s exec `readinessProbe`.
/// `["CMD-SHELL", "<script>"]` -> `sh -c <script>`; `["CMD", a, b]` ->
/// `[a, b]`; `["NONE"]` -> no probe.
fn health_probe(hc: &HealthCheckSpec) -> Option<Probe> {
    let cmd = match hc.command.first().map(String::as_str) {
        Some("NONE") | None => return None,
        Some("CMD-SHELL") => vec![
            "sh".to_string(),
            "-c".to_string(),
            hc.command[1..].join(" "),
        ],
        Some("CMD") => hc.command[1..].to_vec(),
        _ => hc.command.clone(),
    };
    if cmd.is_empty() {
        return None;
    }
    Some(Probe {
        exec: Some(ExecAction { command: Some(cmd) }),
        period_seconds: Some(hc.interval_seconds.max(1) as i32),
        timeout_seconds: Some(hc.timeout_seconds.max(1) as i32),
        failure_threshold: Some(hc.retries.max(1) as i32),
        initial_delay_seconds: Some(hc.start_period_seconds as i32),
        ..Probe::default()
    })
}

/// A DNS-1123 volume name derived from the mount source, stable so two
/// containers mounting the same source share one emptyDir.
fn volume_name_for(source: &str) -> String {
    let slug = names::label_safe(source);
    let short: String = slug.chars().rev().take(40).collect::<String>();
    let short: String = short.chars().rev().collect();
    let h = names::simple_hex12(source);
    let s = short.trim_matches('-');
    if s.is_empty() {
        format!("vol-{h}")
    } else {
        format!("vol-{s}-{h}")
    }
}

fn unique_dns_name(original: &str, idx: usize, used: &mut HashSet<String>) -> String {
    let base = names::label_safe(original);
    let base = if base.is_empty() {
        format!("c{idx}")
    } else {
        base.chars().take(50).collect::<String>()
    };
    let base = base.trim_matches('-').to_string();
    let base = if base.is_empty() {
        format!("c{idx}")
    } else {
        base
    };
    if used.insert(base.clone()) {
        return base;
    }
    let mut n = 1;
    loop {
        let candidate = format!("{base}-{n}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{DependsOn, PortMapping};

    fn plan(name: &str, essential: bool) -> ContainerPlan {
        ContainerPlan {
            container_name: name.to_string(),
            image: "public.ecr.aws/nginx/nginx:latest".to_string(),
            env: vec![],
            entry_point: vec![],
            command: vec![],
            secrets_refs: vec![],
            essential,
            has_task_role: false,
            port_mappings: vec![],
            network_mode: Some("awsvpc".to_string()),
            depends_on: vec![],
            health_check: None,
            volume_mounts: vec![],
            ulimits: vec![],
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        }
    }

    fn build(resolved: &[(ContainerPlan, Vec<(String, String)>)]) -> (Pod, Vec<ContainerMapEntry>) {
        build_task_pod(
            "fakecloud-ecs-t-abc",
            "fc",
            "fakecloud-1",
            None,
            "fakecloud.fc.svc",
            4566,
            "task-1",
            resolved,
        )
    }

    fn rc(name: &str, essential: bool, exit: Option<i64>) -> RunningContainer {
        RunningContainer {
            name: name.into(),
            container_id: name.into(),
            essential,
            exit_code: exit,
            network_bindings: vec![],
            image_digest: None,
        }
    }

    #[test]
    fn failed_init_before_essential_reports_failure_not_zero() {
        // Non-essential init exited 1; essential app never started (no exit).
        let containers = vec![rc("migrate", false, Some(1)), rc("app", true, None)];
        assert_eq!(task_outcome(&containers, false), (1, "TaskFailedToStart"));
    }

    #[test]
    fn essential_exit_governs_outcome() {
        let containers = vec![rc("app", true, Some(0)), rc("side", false, Some(3))];
        assert_eq!(
            task_outcome(&containers, false),
            (0, "EssentialContainerExited")
        );
    }

    #[test]
    fn no_essential_completes_with_first_exit() {
        let containers = vec![rc("job", false, Some(0))];
        assert_eq!(task_outcome(&containers, false), (0, "TaskCompleted"));
    }

    #[test]
    fn stopped_externally_with_unknown_exits_is_sigkill() {
        let containers = vec![rc("app", true, None)];
        assert_eq!(
            task_outcome(&containers, true),
            (137, "EssentialContainerExited")
        );
    }

    #[test]
    fn single_container_task_maps_to_one_app_container() {
        let (pod, map) = build(&[(plan("web", true), vec![])]);
        let spec = pod.spec.unwrap();
        assert_eq!(spec.containers.len(), 1);
        assert!(spec.init_containers.is_none());
        assert_eq!(map[0].original, "web");
        assert_eq!(spec.containers[0].name, "web");
        assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
    }

    #[test]
    fn complete_dependency_target_becomes_init_container() {
        let mut web = plan("web", true);
        web.depends_on = vec![DependsOn {
            container_name: "migrate".to_string(),
            condition: DependsOnCondition::Success,
        }];
        let migrate = plan("migrate", false);
        let (pod, _) = build(&[(web, vec![]), (migrate, vec![])]);
        let spec = pod.spec.unwrap();
        let inits = spec.init_containers.unwrap();
        assert_eq!(inits.len(), 1);
        assert_eq!(inits[0].name, "migrate");
        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].name, "web");
    }

    #[test]
    fn ecr_image_translated_and_env_command_mapped() {
        let mut p = plan("app", true);
        p.image = "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag".to_string();
        p.entry_point = vec!["/bin/sh".to_string()];
        p.command = vec!["-c".to_string(), "echo hi".to_string()];
        p.env = vec![("FOO".to_string(), "bar".to_string())];
        let (pod, _) = build(&[(p, vec![("FOO".to_string(), "bar".to_string())])]);
        let c = &pod.spec.unwrap().containers[0];
        assert_eq!(c.image.as_deref(), Some("fakecloud.fc.svc:4566/repo:tag"));
        assert_eq!(c.command.as_ref().unwrap(), &vec!["/bin/sh".to_string()]);
        assert_eq!(
            c.args.as_ref().unwrap(),
            &vec!["-c".to_string(), "echo hi".to_string()]
        );
        assert!(c.env.as_ref().unwrap().iter().any(|e| e.name == "FOO"));
    }

    #[test]
    fn ports_and_health_probe_mapped() {
        let mut p = plan("web", true);
        p.port_mappings = vec![PortMapping {
            container_port: 8080,
            host_port: 0,
            protocol: "tcp".to_string(),
        }];
        p.health_check = Some(HealthCheckSpec {
            command: vec![
                "CMD-SHELL".to_string(),
                "curl -f localhost:8080".to_string(),
            ],
            interval_seconds: 10,
            timeout_seconds: 3,
            retries: 3,
            start_period_seconds: 5,
        });
        let (pod, _) = build(&[(p, vec![])]);
        let c = &pod.spec.unwrap().containers[0];
        assert_eq!(c.ports.as_ref().unwrap()[0].container_port, 8080);
        let probe = c.readiness_probe.as_ref().unwrap();
        let cmd = probe.exec.as_ref().unwrap().command.as_ref().unwrap();
        assert_eq!(cmd[0], "sh");
        assert_eq!(cmd[1], "-c");
        assert!(cmd[2].contains("curl"));
        assert_eq!(probe.period_seconds, Some(10));
    }

    #[test]
    fn duplicate_container_names_are_made_unique() {
        let (_, map) = build(&[
            (plan("My_App", true), vec![]),
            (plan("My.App", false), vec![]),
        ]);
        assert_eq!(map[0].k8s_name, "my-app");
        assert_eq!(map[1].k8s_name, "my-app-1");
    }

    #[test]
    fn shared_mount_source_yields_one_volume() {
        let mut a = plan("a", true);
        let mut b = plan("b", true);
        let vm = crate::runtime::VolumeMount {
            source: "scratch".to_string(),
            container_path: "/data".to_string(),
            read_only: false,
        };
        a.volume_mounts = vec![vm.clone()];
        b.volume_mounts = vec![vm];
        let (pod, _) = build(&[(a, vec![]), (b, vec![])]);
        let vols = pod.spec.unwrap().volumes.unwrap();
        assert_eq!(vols.len(), 1, "same source should share one volume");
    }
}
