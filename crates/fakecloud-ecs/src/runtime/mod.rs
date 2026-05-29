//! Docker/Podman-based ECS task execution.
//!
//! Mirrors the Lambda `ContainerRuntime` approach (auto-detect CLI, forward
//! localhost → host.docker.internal) but scoped for ECS's different
//! lifecycle: tasks are ephemeral, so there is no warm-container pool. Each
//! `run_task` spawns a background tokio task that pulls the image, starts
//! the container, waits for exit, captures logs, and updates shared ECS
//! state in place.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use chrono::Utc;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_logs::ingest::{append_events, IngestEvent};
use fakecloud_logs::SharedLogsState;
use fakecloud_secretsmanager::SharedSecretsManagerState;
use fakecloud_ssm::SharedSsmState;
use parking_lot::RwLock;
use tempfile::TempDir;
use tokio::process::Command;

use crate::state::{LifecycleEvent, SharedEcsState};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container CLI not found (tried docker, podman)")]
    NoCli,
    #[error("image pull failed: {0}")]
    ImagePull(String),
    #[error("container start failed: {0}")]
    ContainerStart(String),
    #[error("docker wait failed: {0}")]
    Wait(String),
}

/// Docker/Podman executor for ECS tasks.
pub struct EcsRuntime {
    cli: String,
    /// Container-to-host networking resolution (host alias, `--add-host`
    /// arg, sibling-container address) shared with the other runtimes via
    /// [`fakecloud_core::container_net`]. Carries the issue #1539 podman +
    /// in-container fixes.
    net: fakecloud_core::container_net::HostNetworking,
    /// Port the main fakecloud server bound to. Used to translate AWS
    /// ECR URIs (`<acct>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`) to
    /// the local OCI v2 endpoint (`127.0.0.1:<port>/<repo>:<tag>`) so
    /// tasks can pull images pushed to fakecloud's own ECR.
    server_port: u16,
    /// Isolated DOCKER_CONFIG dir pre-populated with Basic auth for
    /// `127.0.0.1:<port>`; keeps the host user's `~/.docker/config.json`
    /// untouched and lets `docker pull` succeed against fakecloud ECR
    /// without a prior `aws ecr get-login-password | docker login`.
    docker_config: Option<Arc<TempDir>>,
    /// Tracks per-task lists of `(container_name, docker_container_id)` so
    /// `stop_task` can kill every container backing a task — multi-container
    /// task definitions launch one docker container per `containerDefinitions`
    /// entry, all of which must be torn down on stop.
    containers: RwLock<std::collections::HashMap<String, Vec<(String, String)>>>,
    /// Cross-service delivery bus — emits `aws.ecs` EventBridge events
    /// on task state transitions when wired. `None` if the server started
    /// without EventBridge configured (or for unit tests).
    delivery_bus: Option<Arc<DeliveryBus>>,
    /// CloudWatch Logs state — when set, tasks whose container definition
    /// declares the `awslogs` log driver get their captured stdout/stderr
    /// forwarded to a log group/stream under this shared state.
    logs_state: Option<SharedLogsState>,
    /// SecretsManager state for resolving `containerDefinition.secrets[]`
    /// entries whose `valueFrom` is a SecretsManager ARN.
    secretsmanager_state: Option<SharedSecretsManagerState>,
    /// SSM Parameter Store state for resolving `secrets[]` entries whose
    /// `valueFrom` is an SSM parameter ARN.
    ssm_state: Option<SharedSsmState>,
}

mod config;
mod lb;
mod monitoring;
mod secrets;
mod task_lifecycle;

impl EcsRuntime {
    /// Auto-detect Docker or Podman. Returns `None` if neither is
    /// available. Honours `FAKECLOUD_CONTAINER_CLI` for explicit override.
    /// `server_port` is the port the main fakecloud server bound to;
    /// needed to resolve AWS ECR URIs against the local OCI v2 registry.
    pub fn new(server_port: u16) -> Option<Self> {
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);
        let docker_config = build_local_registry_docker_config(server_port).map(Arc::new);
        Some(Self {
            cli,
            net,
            server_port,
            docker_config,
            containers: RwLock::new(std::collections::HashMap::new()),
            delivery_bus: None,
            logs_state: None,
            secretsmanager_state: None,
            ssm_state: None,
        })
    }

    /// Wire EventBridge delivery so task state transitions emit
    /// `aws.ecs` / `ECS Task State Change` events.
    pub fn with_delivery_bus(mut self, bus: Arc<DeliveryBus>) -> Self {
        self.delivery_bus = Some(bus);
        self
    }

    /// Wire CloudWatch Logs state so tasks using the `awslogs` driver
    /// get their captured stdout/stderr forwarded.
    pub fn with_logs(mut self, logs: SharedLogsState) -> Self {
        self.logs_state = Some(logs);
        self
    }
}

/// Per-container launch plan derived from a task definition.
#[derive(Clone, Debug)]
pub(crate) struct ContainerPlan {
    pub(crate) container_name: String,
    pub(crate) image: String,
    pub(crate) env: Vec<(String, String)>,
    pub(crate) entry_point: Vec<String>,
    pub(crate) command: Vec<String>,
    pub(crate) secrets_refs: Vec<(String, String)>,
    pub(crate) essential: bool,
    pub(crate) has_task_role: bool,
    /// Port mappings parsed from the task definition. Each entry becomes
    /// a `--publish containerPort:hostPort/protocol` flag on the docker
    /// run command (except for `awsvpc`, where ports are exposed via the
    /// per-task ENI rather than the docker host's port table).
    pub(crate) port_mappings: Vec<PortMapping>,
    /// Task-level network mode propagated to every container plan so the
    /// argv builder can decide whether to emit `--publish` flags. Real
    /// ECS treats `awsvpc` as "container is on its own ENI"; the
    /// equivalent in fakecloud is "don't publish to the host".
    pub(crate) network_mode: Option<String>,
    /// Container dependencies parsed from `dependsOn[]`. Each entry pairs
    /// the target container name with the condition that must be observed
    /// before this container is launched: `START` (target exists/running),
    /// `COMPLETE` (target exited, any code), `SUCCESS` (target exited with
    /// code 0), or `HEALTHY` (target's docker `Health.Status` is `healthy`).
    /// Used both to topologically order the launch loop and to gate each
    /// `docker run` on the upstream condition.
    pub(crate) depends_on: Vec<DependsOn>,
    /// Parsed `healthCheck` from the task definition. Translated into
    /// docker `--health-*` flags on `docker run` so the container's
    /// health is observable via `docker inspect .State.Health.Status`.
    /// `None` when the task definition doesn't declare a healthCheck;
    /// the container's `healthStatus` then stays `UNKNOWN` (matching ECS
    /// behaviour for tasks without a health probe).
    pub(crate) health_check: Option<HealthCheckSpec>,
    /// Volume mounts resolved by joining the container definition's
    /// `mountPoints[]` with the task definition's `volumes[]`. Each entry
    /// renders as one `-v` flag on the `docker run` invocation. Empty when
    /// the container has no mount points or no matching volume entries.
    pub(crate) volume_mounts: Vec<VolumeMount>,
    /// Parsed `ulimits` from the container definition. Each entry becomes
    /// `--ulimit <name>=<soft>:<hard>` on `docker run`.
    pub(crate) ulimits: Vec<Ulimit>,
    /// Parsed `linuxParameters` from the container definition. Emits
    /// `--cap-add`, `--cap-drop`, `--device`, `--init`, `--shm-size`,
    /// `--sysctl`, `--tmpfs`, `--privileged`, and `--read-only` flags.
    pub(crate) linux_parameters: Option<LinuxParameters>,
    /// `stopTimeout` in seconds. Becomes `--stop-timeout <N>` on `docker run`.
    pub(crate) stop_timeout: Option<u32>,
    /// `user` from the container definition. Becomes `--user <value>`.
    pub(crate) user: Option<String>,
    /// `workingDirectory` from the container definition. Becomes `--workdir`.
    pub(crate) working_directory: Option<String>,
    /// `tty` from the container definition. Emits `--tty` when true.
    pub(crate) tty: bool,
    /// `interactive` from the container definition. Emits `--interactive` when true.
    pub(crate) interactive: bool,
    /// `readonlyRootFilesystem` from the container definition. Emits `--read-only` when true.
    pub(crate) readonly_rootfs: bool,
}

/// One parsed `dependsOn[]` entry on a container. Pairs the upstream
/// container name with the condition that must hold before the dependent
/// container is launched. AWS spells the conditions `START`, `COMPLETE`,
/// `SUCCESS`, `HEALTHY` and treats anything else as an error at register
/// time — we mirror that in [`parse_depends_on`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DependsOn {
    pub container_name: String,
    pub condition: DependsOnCondition,
}

/// `dependsOn[].condition` from the task definition. The variants map
/// 1:1 to AWS's documented values; the launch loop polls docker for the
/// matching predicate before starting the dependent container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DependsOnCondition {
    /// Upstream container has been started (docker container exists and
    /// is either running or has exited).
    Start,
    /// Upstream container has exited (any exit code).
    Complete,
    /// Upstream container has exited with code 0.
    Success,
    /// Upstream container's `Health.Status` is `healthy`. When the
    /// upstream has no healthCheck configured, AWS treats this as
    /// immediately satisfied — we do the same.
    Healthy,
}

impl DependsOnCondition {
    /// Parse the AWS-spelled condition string. Returns `None` for
    /// unrecognised values so callers can surface a `ClientException`
    /// at register time.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "START" => Some(Self::Start),
            "COMPLETE" => Some(Self::Complete),
            "SUCCESS" => Some(Self::Success),
            "HEALTHY" => Some(Self::Healthy),
            _ => None,
        }
    }

    /// AWS-spelled string for this condition. Used in user-facing error
    /// messages so timeout/dependency-failed reasons echo back the same
    /// value the user wrote in their task definition.
    pub fn as_aws_str(self) -> &'static str {
        match self {
            Self::Start => "START",
            Self::Complete => "COMPLETE",
            Self::Success => "SUCCESS",
            Self::Healthy => "HEALTHY",
        }
    }
}

/// Container health check parsed from the ECS task definition. Each
/// field maps 1:1 to a docker `--health-*` flag on `docker run`. AWS
/// defaults: interval=30s, timeout=5s, retries=3, startPeriod=0s — we
/// preserve those defaults at parse time so the argv builder always
/// has concrete values to emit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HealthCheckSpec {
    /// `command[]` from the task definition. The first element selects
    /// the docker syntax: `CMD-SHELL` => `--health-cmd <rest joined by space>`,
    /// `CMD` => `--health-cmd <rest joined by space>` (still routed to
    /// `--health-cmd` because docker doesn't accept argv-form here),
    /// `NONE` => no flag emitted (caller skips emitting healthcheck).
    pub command: Vec<String>,
    pub interval_seconds: u32,
    pub timeout_seconds: u32,
    pub retries: u32,
    pub start_period_seconds: u32,
}

/// One entry in a container's `portMappings`. Mirrors the AWS shape so
/// [`build_run_argv`] and the `networkBindings` response can share the
/// same parsed representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PortMapping {
    pub container_port: u16,
    /// `0` (or unset in the source JSON) means "use the same value as
    /// containerPort" — host-mode default per AWS docs.
    pub host_port: u16,
    /// Lower-case `tcp` / `udp`. Defaults to `tcp` when omitted.
    pub protocol: String,
}

/// One resolved `mountPoints` entry on a container plan. Computed at
/// launch by joining the container definition's `mountPoints` against the
/// task definition's `volumes` array. Each entry becomes a single
/// `-v <source>:<containerPath>[:ro]` flag on `docker run`.
///
/// Source resolution by volume kind:
/// - **host bind** (`volume.host.sourcePath` set): bind the host path
///   into the container at `containerPath`.
/// - **EFS** (`efsVolumeConfiguration` set): bind a host-side stub
///   directory at `/tmp/fakecloud/efs/<filesystemId>[/<rootDirectory>]`
///   so multiple tasks targeting the same filesystem id can share state
///   the way real EFS would. The stub directory is created with
///   `mkdir -p` ahead of `docker run`.
/// - **FSx for Windows** (`fsxWindowsFileServerVolumeConfiguration` set):
///   stub directory at `/tmp/fakecloud/fsx/<filesystemId>/<rootDirectory>`
///   created the same way as EFS.
/// - **Docker named volume** (`dockerVolumeConfiguration` set): pass the
///   volume name through to docker as a named volume reference.
/// - **Bare volume** (only `name` set, no host config): treated as an
///   anonymous docker volume for that task — matches AWS's "Docker
///   volumes" default scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VolumeMount {
    /// Left side of `-v`: a host path, a docker named volume, or a stub
    /// directory under `/tmp/fakecloud/{efs,fsx}/...` for shared FS
    /// emulation.
    pub source: String,
    /// Container-side path, taken verbatim from the container
    /// definition's `mountPoints[].containerPath`.
    pub container_path: String,
    /// `mountPoints[].readOnly` honoured: when true, append `:ro` to the
    /// `-v` flag so the bind/named volume is read-only inside the
    /// container. Defaults to false (read-write) when omitted.
    pub read_only: bool,
}

/// One `ulimits` entry. Becomes `--ulimit <name>=<soft>:<hard>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Ulimit {
    pub name: String,
    pub soft_limit: i32,
    pub hard_limit: i32,
}

/// One `linuxParameters.devices` entry. Becomes `--device <hostPath>:<containerPath><permissions>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Device {
    pub host_path: String,
    pub container_path: String,
    pub permissions: String,
}

/// One `linuxParameters.sysctl` entry. Becomes `--sysctl <name>=<value>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Sysctl {
    pub name: String,
    pub value: String,
}

/// Parsed `linuxParameters` from the container definition.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct LinuxParameters {
    pub capabilities_add: Vec<String>,
    pub capabilities_drop: Vec<String>,
    pub devices: Vec<Device>,
    pub init_process_enabled: bool,
    pub shared_memory_size: Option<i32>,
    pub sysctls: Vec<Sysctl>,
    pub tmpfs: Vec<Tmpfs>,
    pub privileged: bool,
}

/// One `linuxParameters.tmpfs` entry. Becomes `--tmpfs <containerPath>:size=<size>M<,options>*`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tmpfs {
    pub container_path: String,
    pub size: i32,
    pub mount_options: Vec<String>,
}

#[derive(Clone, Debug)]
struct ResolvedContainerPlan {
    plan: ContainerPlan,
    env: Vec<(String, String)>,
}

/// Result of waiting for a task's lifetime-determining container.
#[derive(Clone, Debug)]
struct TaskExitOutcome {
    /// Index into the started-containers list of the container whose exit
    /// closed out the task. `None` only in degenerate cases — kept as
    /// `Option` so `final_containers` indexing stays explicit.
    exited_index: Option<usize>,
    exit_code: i64,
    stop_code: &'static str,
}

/// Per-container record persisted on the task. Mirrors the AWS Container
/// shape but tracks the docker-side container id alongside ECS metadata.
#[derive(Clone, Debug)]
pub(crate) struct RunningContainer {
    pub(crate) name: String,
    pub(crate) container_id: String,
    pub(crate) essential: bool,
    pub(crate) exit_code: Option<i64>,
    /// Resolved `networkBindings` for DescribeTasks. Computed from the
    /// task definition's `portMappings` at launch and surfaced verbatim
    /// in the per-container response.
    pub(crate) network_bindings: Vec<serde_json::Value>,
    /// Image digest captured from `docker inspect` after pull. AWS
    /// surfaces this on the Container response so callers can pin which
    /// exact image revision a task is running. `None` when the inspect
    /// failed or the CLI didn't expose `RepoDigests`.
    pub(crate) image_digest: Option<String>,
}

/// Pure decision: does the current set of containers warrant stopping
/// the task? Returns true when any essential container has exited, or
/// when every container has exited (regardless of essential). Mirrors
/// AWS ECS task lifetime semantics.
pub(crate) fn task_should_stop(containers: &[RunningContainer]) -> bool {
    if containers.is_empty() {
        return true;
    }
    let any_essential_exited = containers
        .iter()
        .any(|c| c.essential && c.exit_code.is_some());
    if any_essential_exited {
        return true;
    }
    containers.iter().all(|c| c.exit_code.is_some())
}

fn build_container_plans(
    state: &SharedEcsState,
    account_id: &str,
    task_id: &str,
    _server_port: u16,
) -> Result<Vec<ContainerPlan>, RuntimeError> {
    let accounts = state.read();
    let s = accounts
        .get(account_id)
        .ok_or_else(|| RuntimeError::ContainerStart("account missing".into()))?;
    let task = s
        .tasks
        .get(task_id)
        .ok_or_else(|| RuntimeError::ContainerStart("task missing".into()))?;
    if task.containers.is_empty() {
        return Err(RuntimeError::ContainerStart(
            "task has no containers".into(),
        ));
    }
    let has_task_role = task.task_role_arn.is_some();
    let task_def = s
        .task_definitions
        .get(&task.family)
        .and_then(|revs| revs.get(&task.revision));
    let network_mode = task_def.and_then(|td| td.network_mode.clone());
    // Index `volumes[]` by name so each container's `mountPoints[]` can
    // resolve its volume in O(1). Real ECS rejects mountPoints that
    // reference an undeclared volume at register time; we don't yet, so
    // unresolved names just produce zero mounts at launch.
    let volumes_by_name: std::collections::HashMap<String, &serde_json::Value> = task_def
        .map(|td| {
            td.volumes
                .iter()
                .filter_map(|v| {
                    let name = v.get("name").and_then(|n| n.as_str())?;
                    Some((name.to_string(), v))
                })
                .collect()
        })
        .unwrap_or_default();
    let mut plans = Vec::with_capacity(task.containers.len());
    for container in &task.containers {
        let def = find_container_definition(s, &task.family, task.revision, &container.name);
        let secrets_refs = def
            .as_ref()
            .and_then(|d| d.get("secrets").and_then(|v| v.as_array()).cloned())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| {
                        let name = e.get("name").and_then(|v| v.as_str())?.to_string();
                        let value_from = e.get("valueFrom").and_then(|v| v.as_str())?.to_string();
                        Some((name, value_from))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let str_array = |key: &str| -> Vec<String> {
            def.as_ref()
                .and_then(|d| d.get(key).and_then(|v| v.as_array()).cloned())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        let env = def
            .as_ref()
            .and_then(|d| d.get("environment").and_then(|v| v.as_array()).cloned())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| {
                        let k = e.get("name").and_then(|v| v.as_str())?;
                        let v = e.get("value").and_then(|v| v.as_str()).unwrap_or("");
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let port_mappings = def
            .as_ref()
            .and_then(|d| d.get("portMappings").and_then(|v| v.as_array()).cloned())
            .map(|arr| {
                arr.iter()
                    .filter_map(parse_port_mapping)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let depends_on = def
            .as_ref()
            .and_then(|d| d.get("dependsOn").and_then(|v| v.as_array()).cloned())
            .map(|arr| {
                arr.iter()
                    .filter_map(parse_depends_on_entry)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let health_check = def
            .as_ref()
            .and_then(|d| d.get("healthCheck"))
            .and_then(parse_health_check);
        let volume_mounts = def
            .as_ref()
            .and_then(|d| d.get("mountPoints").and_then(|v| v.as_array()).cloned())
            .map(|arr| {
                arr.iter()
                    .filter_map(|mp| resolve_mount_point(mp, &volumes_by_name))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let ulimits = def
            .as_ref()
            .and_then(|d| d.get("ulimits").and_then(|v| v.as_array()).cloned())
            .map(|arr| arr.iter().filter_map(parse_ulimit).collect::<Vec<_>>())
            .unwrap_or_default();
        let linux_parameters = def
            .as_ref()
            .and_then(|d| d.get("linuxParameters"))
            .and_then(parse_linux_parameters);
        let stop_timeout = def.as_ref().and_then(|d| {
            d.get("stopTimeout")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32)
        });
        let user = def
            .as_ref()
            .and_then(|d| d.get("user").and_then(|v| v.as_str()).map(String::from));
        let working_directory = def.as_ref().and_then(|d| {
            d.get("workingDirectory")
                .and_then(|v| v.as_str())
                .map(String::from)
        });
        let tty = def
            .as_ref()
            .and_then(|d| d.get("tty").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        let interactive = def
            .as_ref()
            .and_then(|d| d.get("interactive").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        let readonly_rootfs = def
            .as_ref()
            .and_then(|d| d.get("readonlyRootFilesystem").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        plans.push(ContainerPlan {
            container_name: container.name.clone(),
            image: container.image.clone(),
            env,
            entry_point: str_array("entryPoint"),
            command: str_array("command"),
            secrets_refs,
            essential: container.essential,
            has_task_role,
            port_mappings,
            network_mode: network_mode.clone(),
            depends_on,
            health_check,
            volume_mounts,
            ulimits,
            linux_parameters,
            stop_timeout,
            user,
            working_directory,
            tty,
            interactive,
            readonly_rootfs,
        });
    }
    let plans = topo_sort_plans(plans);
    Ok(plans)
}

/// Resolve one `mountPoints[]` entry against the indexed task-definition
/// volumes. Returns `None` when:
/// - the entry has no `containerPath` or `sourceVolume`,
/// - the named volume isn't declared on the task definition.
///
/// Returns `Some(VolumeMount)` for every supported volume kind:
/// host bind, EFS, FSx, named docker volume, anonymous docker volume.
fn resolve_mount_point(
    mount_point: &serde_json::Value,
    volumes_by_name: &std::collections::HashMap<String, &serde_json::Value>,
) -> Option<VolumeMount> {
    let container_path = mount_point
        .get("containerPath")
        .and_then(|v| v.as_str())?
        .to_string();
    let source_volume = mount_point.get("sourceVolume").and_then(|v| v.as_str())?;
    let read_only = mount_point
        .get("readOnly")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let volume = volumes_by_name.get(source_volume)?;
    let source = resolve_volume_source(source_volume, volume)?;
    Some(VolumeMount {
        source,
        container_path,
        read_only,
    })
}

/// Map a single task-definition `volumes[]` entry to the source side of a
/// `docker run -v` flag. The matching here mirrors the AWS volume kinds:
///
/// 1. `host.sourcePath` -> use that path directly (bind mount).
/// 2. `efsVolumeConfiguration.fileSystemId` -> stub directory under
///    `/tmp/fakecloud/efs/<filesystemId>[/<rootDirectory>]`. Created with
///    `mkdir -p` so different tasks targeting the same filesystem id
///    share the same host directory, matching real EFS's "many tasks,
///    one filesystem" semantics.
/// 3. `fsxWindowsFileServerVolumeConfiguration.fileSystemId` -> stub
///    directory under `/tmp/fakecloud/fsx/<filesystemId>/<rootDirectory>`.
/// 4. `dockerVolumeConfiguration` -> the volume `name` itself (named
///    docker volume; docker creates it on first reference).
/// 5. Bare entry (only `name`) -> the volume `name` as an anonymous
///    docker volume reference, matching AWS's "Docker volumes" default.
///
/// Returns `None` when the configuration is malformed (e.g. EFS without
/// a fileSystemId).
fn resolve_volume_source(name: &str, volume: &serde_json::Value) -> Option<String> {
    if let Some(host) = volume.get("host") {
        if let Some(path) = host.get("sourcePath").and_then(|v| v.as_str()) {
            // Empty sourcePath means "anonymous host volume" — fall
            // through to the named-volume default below.
            if !path.is_empty() {
                ensure_dir_exists(path);
                return Some(path.to_string());
            }
        }
    }
    if let Some(efs) = volume.get("efsVolumeConfiguration") {
        let fs_id = efs.get("fileSystemId").and_then(|v| v.as_str())?;
        let root = efs
            .get("rootDirectory")
            .and_then(|v| v.as_str())
            .unwrap_or("/");
        return Some(shared_volume_name("efs", fs_id, root));
    }
    if let Some(fsx) = volume.get("fsxWindowsFileServerVolumeConfiguration") {
        let fs_id = fsx.get("fileSystemId").and_then(|v| v.as_str())?;
        let root = fsx
            .get("rootDirectory")
            .and_then(|v| v.as_str())
            .unwrap_or("/");
        return Some(shared_volume_name("fsx", fs_id, root));
    }
    if volume.get("dockerVolumeConfiguration").is_some() {
        // Named docker volume — docker auto-creates it on first
        // reference. Pass the volume name through verbatim.
        return Some(name.to_string());
    }
    // Bare volume entry: anonymous docker volume keyed by name.
    Some(name.to_string())
}

/// Compose the docker **named-volume** name for an EFS/FSx volume. A
/// single shared volume per filesystem id when `rootDirectory` is unset
/// or `/` (the EFS default mount target); otherwise the rootDirectory is
/// folded into the name so distinct mount targets within one filesystem
/// stay isolated. A docker named volume lives on the daemon rather than a
/// host path, so tasks share state correctly *and* it works when
/// fakecloud itself runs in a container (`FAKECLOUD_IN_CONTAINER=1`),
/// where a host-path stub created inside fakecloud's own filesystem would
/// resolve to an empty dir against the host daemon (issue #1539, bug 0.6).
/// The segments are sanitized to docker's volume-name charset.
fn shared_volume_name(kind: &str, fs_id: &str, root: &str) -> String {
    let trimmed = root.trim_start_matches('/').trim_end_matches('/');
    let fs_id = sanitize_volume_segment(fs_id);
    if trimmed.is_empty() {
        format!("fakecloud-{kind}-{fs_id}")
    } else {
        format!(
            "fakecloud-{kind}-{fs_id}-{}",
            sanitize_volume_segment(trimmed)
        )
    }
}

/// Map an arbitrary string to docker's volume-name charset by replacing
/// every character outside `[A-Za-z0-9_.-]` with `-`.
fn sanitize_volume_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Best-effort `mkdir -p` so the EFS/FSx stub path exists before the
/// first task tries to bind-mount it. Failures are ignored — docker
/// will surface a clear error on the run, and unit tests don't have a
/// writable `/tmp/fakecloud` in every sandbox.
fn ensure_dir_exists(path: &str) {
    let _ = std::fs::create_dir_all(path);
}

/// Parse one `dependsOn[]` entry. Returns `None` for malformed entries
/// (missing `containerName`, unrecognised `condition`) so the caller
/// can drop them silently from the launch plan — register-time
/// validation already rejects bad values; this is a defensive fallback.
fn parse_depends_on_entry(value: &serde_json::Value) -> Option<DependsOn> {
    let container_name = value
        .get("containerName")
        .and_then(|v| v.as_str())?
        .to_string();
    let raw_condition = value.get("condition").and_then(|v| v.as_str())?;
    let condition = DependsOnCondition::parse(raw_condition)?;
    Some(DependsOn {
        container_name,
        condition,
    })
}

/// Topologically sort container plans so `dependsOn` dependencies start
/// before their dependants. Implements Kahn's algorithm with stable order:
/// when multiple plans are ready, we keep their original declaration
/// index, so a task without any dependsOn launches in the same order the
/// user wrote in the task definition. Cycles fall through with the
/// remaining plans appended in original order — the runtime will still
/// launch every container; it just can't guarantee dependency ordering
/// in that degenerate case. Cycles are rejected at register time
/// (RegisterTaskDefinition -> validate_depends_on_acyclic), so reaching
/// that branch from a real launch path means a bug elsewhere.
fn topo_sort_plans(plans: Vec<ContainerPlan>) -> Vec<ContainerPlan> {
    use std::collections::{HashMap, HashSet};
    let names: HashSet<String> = plans.iter().map(|p| p.container_name.clone()).collect();
    let index: HashMap<String, usize> = plans
        .iter()
        .enumerate()
        .map(|(i, p)| (p.container_name.clone(), i))
        .collect();
    // in_degree[i] = number of unresolved dependencies for plan i. We
    // ignore depends_on entries that name a container not in the task
    // (real ECS rejects those at register time; our register path doesn't
    // yet, so be defensive here).
    let mut in_degree: Vec<usize> = plans
        .iter()
        .map(|p| {
            p.depends_on
                .iter()
                .filter(|d| names.contains(&d.container_name))
                .count()
        })
        .collect();
    // dependants[i] = indices of plans that depend on plan i.
    let mut dependants: Vec<Vec<usize>> = vec![Vec::new(); plans.len()];
    for (i, p) in plans.iter().enumerate() {
        for d in &p.depends_on {
            if let Some(&di) = index.get(&d.container_name) {
                dependants[di].push(i);
            }
        }
    }
    let mut ordered: Vec<ContainerPlan> = Vec::with_capacity(plans.len());
    let mut emitted: Vec<bool> = vec![false; plans.len()];
    loop {
        // Pick the lowest-index plan whose in_degree is 0 to keep stable
        // order across runs.
        let next = (0..plans.len()).find(|&i| !emitted[i] && in_degree[i] == 0);
        match next {
            Some(i) => {
                emitted[i] = true;
                ordered.push(plans[i].clone());
                for &di in &dependants[i] {
                    if in_degree[di] > 0 {
                        in_degree[di] -= 1;
                    }
                }
            }
            None => break,
        }
    }
    // Cycle: append anything left in original order so we don't drop plans.
    for (i, p) in plans.into_iter().enumerate() {
        if !emitted[i] {
            ordered.push(p);
        }
    }
    ordered
}

/// Validate that `containerDefinitions[].dependsOn[]` graph is acyclic.
/// Real ECS rejects cyclic dependencies at RegisterTaskDefinition time
/// with a `ClientException`; we mirror that. Returns the offending pair
/// of container names so the caller can produce a useful error.
///
/// Operates directly on the raw JSON definitions (rather than parsed
/// `ContainerPlan`s) so register-time validation doesn't have to first
/// build a full plan from a not-yet-stored task definition.
pub(crate) fn find_depends_on_cycle(
    container_definitions: &[serde_json::Value],
) -> Option<(String, String)> {
    use std::collections::HashMap;

    let names: Vec<String> = container_definitions
        .iter()
        .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    let index: HashMap<&str, usize> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); names.len()];
    for (i, cd) in container_definitions.iter().enumerate() {
        if i >= names.len() {
            continue;
        }
        let Some(deps) = cd.get("dependsOn").and_then(|v| v.as_array()) else {
            continue;
        };
        for d in deps {
            let Some(target) = d.get("containerName").and_then(|v| v.as_str()) else {
                continue;
            };
            if let Some(&j) = index.get(target) {
                // Edge: i depends on j -> for cycle DFS we walk from i to j.
                adj[i].push(j);
            }
        }
    }

    // DFS with three-colour marking (white=0, gray=1, black=2). When we
    // hit a gray neighbour we've closed a cycle; report the back-edge as
    // the offending pair.
    let mut state = vec![0u8; names.len()];
    let mut stack: Vec<(usize, usize)> = Vec::new();
    for start in 0..names.len() {
        if state[start] != 0 {
            continue;
        }
        stack.clear();
        stack.push((start, 0));
        state[start] = 1;
        while let Some(&(node, next_edge)) = stack.last() {
            if next_edge < adj[node].len() {
                let nb = adj[node][next_edge];
                stack.last_mut().unwrap().1 += 1;
                match state[nb] {
                    0 => {
                        state[nb] = 1;
                        stack.push((nb, 0));
                    }
                    1 => {
                        return Some((names[node].clone(), names[nb].clone()));
                    }
                    _ => {}
                }
            } else {
                state[node] = 2;
                stack.pop();
            }
        }
    }
    None
}

/// Snapshot of the docker container state we care about for `dependsOn`
/// gating: whether the container exists/started, whether it's exited,
/// its exit code, and (when configured) its health status.
#[derive(Debug, Clone)]
struct InspectedState {
    started: bool,
    exited: bool,
    exit_code: i64,
    health: Option<String>,
}

/// One `docker inspect` call returning every field needed by
/// [`condition_is_met`]. Returns `None` when the container doesn't exist
/// yet or inspect fails — the caller will simply retry on the next poll.
async fn inspect_container_state(cli: &str, container_id: &str) -> Option<InspectedState> {
    // Compose all four fields into a single inspect format so the gate
    // costs one process spawn per poll rather than four.
    let format =
        "{{.State.Status}}|{{.State.Running}}|{{.State.ExitCode}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}<none>{{end}}";
    let out = Command::new(cli)
        .args(["inspect", "-f", format, container_id])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let parts: Vec<&str> = raw.split('|').collect();
    if parts.len() < 4 {
        return None;
    }
    let status = parts[0];
    let running = parts[1] == "true";
    let exit_code: i64 = parts[2].parse().unwrap_or(-1);
    let health = match parts[3] {
        "<none>" | "" => None,
        other => Some(other.to_string()),
    };
    // `created` is the brief moment between docker creating the
    // container and the entrypoint running. Treat anything past
    // `created` as "started" for the START condition.
    let started = running || status == "exited" || status == "running" || status == "dead";
    let exited = status == "exited" || status == "dead";
    Some(InspectedState {
        started,
        exited,
        exit_code,
        health,
    })
}

/// Decide whether the polled `state` satisfies a `dependsOn[].condition`.
/// Encapsulates the AWS semantics so the polling loop is purely
/// mechanical.
fn condition_is_met(condition: DependsOnCondition, state: &InspectedState) -> bool {
    match condition {
        DependsOnCondition::Start => state.started,
        DependsOnCondition::Complete => state.exited,
        DependsOnCondition::Success => state.exited && state.exit_code == 0,
        DependsOnCondition::Healthy => state.health.as_deref() == Some("healthy"),
    }
}

/// Test-only re-export of [`parse_port_mapping`] so sibling test modules
/// can lock in the default-port / default-protocol behaviour without us
/// widening the visibility of the parser itself.
#[cfg(test)]
pub(crate) fn __test_parse_port_mapping(value: &serde_json::Value) -> Option<PortMapping> {
    parse_port_mapping(value)
}

/// Parse a `healthCheck` block from a task definition's container
/// definition. Returns `None` for missing `command` or for a command
/// whose first token is `NONE` (the AWS-documented "disable healthcheck
/// inherited from image" sentinel — emit no flags rather than a `none`
/// healthcheck). Defaults follow AWS: 30s/5s/3/0s.
fn parse_health_check(value: &serde_json::Value) -> Option<HealthCheckSpec> {
    let cmd_arr = value.get("command")?.as_array()?;
    let command: Vec<String> = cmd_arr
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if command.is_empty() {
        return None;
    }
    if command.first().map(|s| s.as_str()) == Some("NONE") {
        return None;
    }
    let read_u32 = |key: &str, default: u32| -> u32 {
        value
            .get(key)
            .and_then(|v| v.as_i64())
            .filter(|n| (0..=u32::MAX as i64).contains(n))
            .map(|n| n as u32)
            .unwrap_or(default)
    };
    Some(HealthCheckSpec {
        command,
        interval_seconds: read_u32("interval", 30),
        timeout_seconds: read_u32("timeout", 5),
        retries: read_u32("retries", 3),
        start_period_seconds: read_u32("startPeriod", 0),
    })
}

/// Parse one `ulimits` entry from the container definition JSON.
fn parse_ulimit(value: &serde_json::Value) -> Option<Ulimit> {
    let name = value.get("name").and_then(|v| v.as_str())?;
    let soft = value
        .get("softLimit")
        .and_then(|v| v.as_i64())
        .filter(|n| *n >= 0)? as i32;
    let hard = value
        .get("hardLimit")
        .and_then(|v| v.as_i64())
        .filter(|n| *n >= 0)? as i32;
    Some(Ulimit {
        name: name.to_string(),
        soft_limit: soft,
        hard_limit: hard,
    })
}

/// Parse `linuxParameters` from the container definition JSON.
fn parse_linux_parameters(value: &serde_json::Value) -> Option<LinuxParameters> {
    let mut lp = LinuxParameters::default();
    if let Some(arr) = value
        .get("capabilities")
        .and_then(|v| v.get("add"))
        .and_then(|v| v.as_array())
    {
        lp.capabilities_add = arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }
    if let Some(arr) = value
        .get("capabilities")
        .and_then(|v| v.get("drop"))
        .and_then(|v| v.as_array())
    {
        lp.capabilities_drop = arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }
    if let Some(arr) = value.get("devices").and_then(|v| v.as_array()) {
        lp.devices = arr.iter().filter_map(parse_device).collect();
    }
    lp.init_process_enabled = value
        .get("initProcessEnabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    lp.shared_memory_size = value
        .get("sharedMemorySize")
        .and_then(|v| v.as_i64())
        .map(|n| n as i32);
    if let Some(arr) = value.get("sysctl").and_then(|v| v.as_array()) {
        lp.sysctls = arr.iter().filter_map(parse_sysctl).collect();
    }
    if let Some(arr) = value.get("tmpfs").and_then(|v| v.as_array()) {
        lp.tmpfs = arr.iter().filter_map(parse_tmpfs).collect();
    }
    lp.privileged = value
        .get("privileged")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(lp)
}

fn parse_device(value: &serde_json::Value) -> Option<Device> {
    let host_path = value.get("hostPath").and_then(|v| v.as_str())?.to_string();
    let container_path = value
        .get("containerPath")
        .and_then(|v| v.as_str())?
        .to_string();
    let permissions = value
        .get("permissions")
        .and_then(|v| v.as_str())
        .unwrap_or("rwm")
        .to_string();
    Some(Device {
        host_path,
        container_path,
        permissions,
    })
}

fn parse_sysctl(value: &serde_json::Value) -> Option<Sysctl> {
    let name = value.get("name").and_then(|v| v.as_str())?.to_string();
    let value_str = value.get("value").and_then(|v| v.as_str())?.to_string();
    Some(Sysctl {
        name,
        value: value_str,
    })
}

fn parse_tmpfs(value: &serde_json::Value) -> Option<Tmpfs> {
    let container_path = value
        .get("containerPath")
        .and_then(|v| v.as_str())?
        .to_string();
    let size = value
        .get("size")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)? as i32;
    let mount_options = value
        .get("mountOptions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Some(Tmpfs {
        container_path,
        size,
        mount_options,
    })
}

/// Render a [`HealthCheckSpec`] into the docker run flags that emulate
/// the equivalent ECS healthCheck. AWS's `command[0]` is a sentinel
/// (`CMD-SHELL`/`CMD`/`NONE`); docker's `--health-cmd` always takes a
/// single shell-string, so we collapse the remaining tokens with spaces
/// for either sentinel — matching how docker itself stringifies HEALTHCHECK
/// CMD ["a","b"] back to a shell string at inspect time.
pub(crate) fn render_health_flags(hc: &HealthCheckSpec) -> Vec<String> {
    if hc.command.len() < 2 {
        return Vec::new();
    }
    let cmd_kind = hc.command[0].as_str();
    if cmd_kind != "CMD" && cmd_kind != "CMD-SHELL" {
        return Vec::new();
    }
    let cmd_string = hc.command[1..].join(" ");
    vec![
        "--health-cmd".into(),
        cmd_string,
        format!("--health-interval={}s", hc.interval_seconds),
        format!("--health-timeout={}s", hc.timeout_seconds),
        format!("--health-retries={}", hc.retries),
        format!("--health-start-period={}s", hc.start_period_seconds),
    ]
}

/// Test-only re-export of [`parse_health_check`] so unit tests in
/// sibling modules can lock in the AWS default-fill behaviour without
/// us widening the parser's visibility.
#[cfg(test)]
pub(crate) fn __test_parse_health_check(value: &serde_json::Value) -> Option<HealthCheckSpec> {
    parse_health_check(value)
}

/// Map a docker `.State.Health.Status` value to the ECS `healthStatus`
/// shape. Docker emits `starting|healthy|unhealthy|none|""` (empty when
/// the image has no HEALTHCHECK and we didn't add one). ECS only knows
/// `HEALTHY|UNHEALTHY|UNKNOWN`, so anything that isn't a clean healthy/
/// unhealthy lands in `UNKNOWN`.
pub(crate) fn docker_health_to_ecs(raw: &str) -> &'static str {
    match raw.trim().to_ascii_lowercase().as_str() {
        "healthy" => "HEALTHY",
        "unhealthy" => "UNHEALTHY",
        _ => "UNKNOWN",
    }
}

/// Parse a single `portMappings[]` entry. Returns `None` for entries
/// that are missing `containerPort` or have a value out of `u16` range.
/// Defaults: `hostPort` -> `containerPort`, `protocol` -> `tcp`.
fn parse_port_mapping(value: &serde_json::Value) -> Option<PortMapping> {
    let container_port = value
        .get("containerPort")
        .and_then(|v| v.as_i64())
        .filter(|n| (0..=u16::MAX as i64).contains(n))? as u16;
    let host_port_raw = value
        .get("hostPort")
        .and_then(|v| v.as_i64())
        .filter(|n| (0..=u16::MAX as i64).contains(n))
        .map(|n| n as u16)
        .unwrap_or(0);
    let host_port = if host_port_raw == 0 {
        container_port
    } else {
        host_port_raw
    };
    let protocol = value
        .get("protocol")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "tcp".to_string());
    Some(PortMapping {
        container_port,
        host_port,
        protocol,
    })
}

/// Build the docker `run` argv for a single container plan. Pure so unit
/// tests can assert on flag ordering / `--publish` translation without
/// shelling out. The returned vector is everything *after* the binary
/// name (i.e. starts with `run`, ends with the user-supplied command
/// args).
pub(crate) fn build_run_argv(
    plan: &ContainerPlan,
    env: &[(String, String)],
    task_id: &str,
    host_alias: &str,
    add_host_arg: Option<&str>,
    run_image: &str,
    awsvpc_network_ready: bool,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    argv.push("run".into());
    argv.push("-d".into());
    argv.push("--name".into());
    argv.push(format!("{}-{}", task_id, plan.container_name));
    argv.push("--label".into());
    argv.push(format!("fakecloud-ecs-task={}", task_id));
    argv.push("--label".into());
    argv.push(format!("fakecloud-ecs-container={}", plan.container_name));
    // Inject `--add-host host.docker.internal:<ip>` only for docker;
    // podman provides `host.containers.internal` natively and rejects
    // the host-gateway mapping (issue #1539).
    if let Some(arg) = add_host_arg {
        argv.push("--add-host".into());
        argv.push(arg.to_string());
    }
    let use_awsvpc_network = plan.network_mode.as_deref() == Some("awsvpc") && awsvpc_network_ready;
    if use_awsvpc_network {
        argv.push("--network".into());
        argv.push(format!("fakecloud-ecs-{}", task_id));
    }
    // `awsvpc` puts the container on a per-task ENI; emulating that on a
    // local docker host means *not* publishing to the host port table.
    // Bridge / host / default network modes still get `--publish`. If
    // the awsvpc per-task network creation failed and we fell back to
    // bridge, we DO want to publish so the container is reachable.
    let publish_ports = !use_awsvpc_network;
    if publish_ports {
        for pm in &plan.port_mappings {
            argv.push("--publish".into());
            argv.push(format!(
                "{}:{}/{}",
                pm.container_port, pm.host_port, pm.protocol
            ));
        }
    }
    if let Some(ref hc) = plan.health_check {
        argv.extend(render_health_flags(hc));
    }
    for (k, v) in env {
        let transformed = v
            .replace("http://127.0.0.1:", &format!("http://{host_alias}:"))
            .replace("https://127.0.0.1:", &format!("https://{host_alias}:"))
            .replace("http://localhost:", &format!("http://{host_alias}:"))
            .replace("https://localhost:", &format!("https://{host_alias}:"));
        argv.push("-e".into());
        argv.push(format!("{}={}", k, transformed));
    }
    // Volume mounts: one `-v` flag per mountPoints entry, with the
    // source resolved from the task definition's `volumes[]`. EFS and
    // FSx stubs were materialised on the host (mkdir -p) before this
    // function returns, so docker can bind them straight in.
    for vm in &plan.volume_mounts {
        argv.push("-v".into());
        let suffix = if vm.read_only { ":ro" } else { "" };
        argv.push(format!("{}:{}{}", vm.source, vm.container_path, suffix));
    }
    for ul in &plan.ulimits {
        argv.push("--ulimit".into());
        argv.push(format!("{}={}:{}", ul.name, ul.soft_limit, ul.hard_limit));
    }
    if let Some(ref lp) = plan.linux_parameters {
        for cap in &lp.capabilities_add {
            argv.push("--cap-add".into());
            argv.push(cap.clone());
        }
        for cap in &lp.capabilities_drop {
            argv.push("--cap-drop".into());
            argv.push(cap.clone());
        }
        for dev in &lp.devices {
            argv.push("--device".into());
            argv.push(format!(
                "{}:{}{}",
                dev.host_path, dev.container_path, dev.permissions
            ));
        }
        if lp.init_process_enabled {
            argv.push("--init".into());
        }
        if let Some(size) = lp.shared_memory_size {
            argv.push("--shm-size".into());
            argv.push(format!("{}m", size));
        }
        for sys in &lp.sysctls {
            argv.push("--sysctl".into());
            argv.push(format!("{}={}", sys.name, sys.value));
        }
        for tmp in &lp.tmpfs {
            let mut opts = tmp.mount_options.join(",");
            if !opts.is_empty() {
                opts = format!(",{}", opts);
            }
            argv.push("--tmpfs".into());
            argv.push(format!("{}:size={}M{}", tmp.container_path, tmp.size, opts));
        }
        if lp.privileged {
            argv.push("--privileged".into());
        }
    }
    if let Some(timeout) = plan.stop_timeout {
        argv.push("--stop-timeout".into());
        argv.push(format!("{}", timeout));
    }
    if let Some(ref user) = plan.user {
        argv.push("--user".into());
        argv.push(user.clone());
    }
    if let Some(ref wd) = plan.working_directory {
        argv.push("--workdir".into());
        argv.push(wd.clone());
    }
    if plan.tty {
        argv.push("--tty".into());
    }
    if plan.interactive {
        argv.push("--interactive".into());
    }
    if plan.readonly_rootfs {
        argv.push("--read-only".into());
    }
    if let Some(first) = plan.entry_point.first() {
        argv.push("--entrypoint".into());
        argv.push(first.clone());
    }
    argv.push(run_image.to_string());
    for arg in plan.entry_point.iter().skip(1) {
        argv.push(arg.clone());
    }
    for arg in &plan.command {
        argv.push(arg.clone());
    }
    argv
}

/// Render `networkBindings` JSON for a launched container. Empty under
/// `awsvpc` (the equivalent info goes on the task's ENI attachments) and
/// for containers without `portMappings`.
pub(crate) fn network_bindings_for(plan: &ContainerPlan) -> Vec<serde_json::Value> {
    if plan.network_mode.as_deref() == Some("awsvpc") {
        return Vec::new();
    }
    plan.port_mappings
        .iter()
        .map(|pm| {
            serde_json::json!({
                "bindIP": "0.0.0.0",
                "containerPort": pm.container_port,
                "hostPort": pm.host_port,
                "protocol": pm.protocol,
            })
        })
        .collect()
}

/// Compute ELBv2 target registrations for a task based on its service's
/// loadBalancers configuration. Returns (target_group_arn, [(target_id, port)])
/// for each target group that should receive this task.
#[allow(clippy::type_complexity)]
pub(crate) fn compute_elbv2_targets(
    ecs_state: &crate::state::EcsState,
    task: &crate::state::Task,
) -> Vec<(String, Vec<(String, Option<i64>)>)> {
    let mut result = Vec::new();
    let Some(group) = task.group.as_deref() else {
        return result;
    };
    let service_name = group.strip_prefix("service:").unwrap_or(group);
    let key = crate::state::EcsState::service_key(&task.cluster_name, service_name);
    let Some(service) = ecs_state.services.get(&key) else {
        return result;
    };

    let network_mode = ecs_state
        .task_definitions
        .get(&task.family)
        .and_then(|revs| revs.get(&task.revision))
        .and_then(|td| td.network_mode.as_deref());

    for lb in &service.load_balancers {
        let tg_arn = lb.get("targetGroupArn").and_then(|v| v.as_str());
        let container_name = lb.get("containerName").and_then(|v| v.as_str());
        let container_port = lb.get("containerPort").and_then(|v| v.as_i64());
        let Some(tg_arn) = tg_arn else { continue };
        let Some(container_name) = container_name else {
            continue;
        };

        let target_id = if network_mode == Some("awsvpc") {
            task.attachments
                .iter()
                .find(|a| a.attachment_type == "eni")
                .and_then(|eni| {
                    eni.details
                        .iter()
                        .find(|d| d.name == "privateIPv4Address")
                        .map(|d| d.value.clone())
                })
        } else {
            Some("127.0.0.1".to_string())
        };

        let port = if network_mode == Some("awsvpc") {
            container_port
        } else {
            task.containers
                .iter()
                .find(|c| c.name == container_name)
                .and_then(|c| {
                    c.network_bindings
                        .iter()
                        .find(|nb| {
                            nb.get("containerPort").and_then(|v| v.as_i64()) == container_port
                        })
                        .and_then(|nb| nb.get("hostPort").and_then(|v| v.as_i64()))
                })
        };

        if let Some(id) = target_id {
            if let Some(entry) = result.iter_mut().find(|(arn, _)| arn == tg_arn) {
                entry.1.push((id, port));
            } else {
                result.push((tg_arn.to_string(), vec![(id, port)]));
            }
        }
    }
    result
}

struct TaskSnapshot {
    task_arn: String,
    cluster_arn: String,
    launch_type: String,
    group: Option<String>,
    task_definition_arn: String,
    containers: serde_json::Value,
}

fn snapshot_task(state: &SharedEcsState, account_id: &str, task_id: &str) -> Option<TaskSnapshot> {
    let accounts = state.read();
    let s = accounts.get(account_id)?;
    let task = s.tasks.get(task_id)?;
    Some(TaskSnapshot {
        task_arn: task.task_arn.clone(),
        cluster_arn: task.cluster_arn.clone(),
        launch_type: task.launch_type.clone(),
        group: task.group.clone(),
        task_definition_arn: task.task_definition_arn.clone(),
        containers: serde_json::Value::Array(
            task.containers
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "containerArn": c.container_arn,
                        "name": c.name,
                        "image": c.image,
                        "lastStatus": c.last_status,
                        "exitCode": c.exit_code,
                        "reason": c.reason,
                    })
                })
                .collect(),
        ),
    })
}

/// Build an isolated docker config directory with Basic auth for
/// fakecloud ECR. Lets `docker pull/push/tag` work against the local OCI
/// v2 registry without requiring the user to run
/// `aws ecr get-login-password | docker login` first. Authorizes both
/// `127.0.0.1` (fakecloud on the host) and `host.docker.internal`
/// (fakecloud in a container, pull URIs rewritten to the sibling host —
/// issue #1539, bug 0.8).
fn build_local_registry_docker_config(server_port: u16) -> Option<TempDir> {
    let dir = TempDir::new().ok()?;
    let auth = base64::engine::general_purpose::STANDARD.encode("AWS:fakecloud-ecs-runtime");
    let config = serde_json::json!({
        "auths": {
            format!("127.0.0.1:{server_port}"): { "auth": auth },
            format!("host.docker.internal:{server_port}"): { "auth": auth },
        }
    });
    std::fs::write(dir.path().join("config.json"), config.to_string()).ok()?;
    Some(dir)
}

fn find_container_definition(
    state: &crate::state::EcsState,
    family: &str,
    revision: i32,
    name: &str,
) -> Option<serde_json::Value> {
    state
        .task_definitions
        .get(family)?
        .get(&revision)?
        .container_definitions
        .iter()
        .find(|c| c.get("name").and_then(|v| v.as_str()) == Some(name))
        .cloned()
}

fn mark_pull_started(state: &SharedEcsState, account_id: &str, task_id: &str) {
    let mut accounts = state.write();
    let Some(s) = accounts.get_mut(account_id) else {
        return;
    };
    let task_arn_cluster = s
        .tasks
        .get(task_id)
        .map(|t| (t.task_arn.clone(), t.cluster_arn.clone()));
    if let Some(task) = s.tasks.get_mut(task_id) {
        task.pull_started_at = Some(Utc::now());
    }
    if let Some((arn, cluster_arn)) = task_arn_cluster {
        s.push_event(LifecycleEvent {
            at: Utc::now(),
            event_type: "PullStarted".into(),
            task_arn: Some(arn),
            cluster_arn: Some(cluster_arn),
            last_status: Some("PENDING".into()),
            detail: serde_json::json!({}),
        });
    }
}

fn mark_pull_stopped(state: &SharedEcsState, account_id: &str, task_id: &str) {
    let mut accounts = state.write();
    let Some(s) = accounts.get_mut(account_id) else {
        return;
    };
    if let Some(task) = s.tasks.get_mut(task_id) {
        task.pull_stopped_at = Some(Utc::now());
    }
}

pub(crate) fn mark_running_multi(
    state: &SharedEcsState,
    account_id: &str,
    task_id: &str,
    started: &[RunningContainer],
) {
    let mut accounts = state.write();
    let Some(s) = accounts.get_mut(account_id) else {
        return;
    };
    let (arn, cluster_arn) = {
        let Some(task) = s.tasks.get_mut(task_id) else {
            return;
        };
        task.last_status = "RUNNING".into();
        task.connectivity = "CONNECTED".into();
        task.connectivity_at = Some(Utc::now());
        task.started_at = Some(Utc::now());
        for rc in started {
            if let Some(c) = task.containers.iter_mut().find(|c| c.name == rc.name) {
                c.runtime_id = Some(rc.container_id.clone());
                c.last_status = "RUNNING".into();
                c.network_bindings = rc.network_bindings.clone();
                if rc.image_digest.is_some() {
                    c.image_digest = rc.image_digest.clone();
                }
            }
        }
        if let Some(cluster) = s.clusters.get_mut(&task.cluster_name) {
            cluster.running_tasks_count += 1;
            if cluster.pending_tasks_count > 0 {
                cluster.pending_tasks_count -= 1;
            }
        }
        if let Some(ref ci_arn) = task.container_instance_arn {
            if let Some(ci) = s
                .container_instances
                .values_mut()
                .find(|ci| ci.container_instance_arn == *ci_arn)
            {
                ci.running_tasks_count += 1;
                if ci.pending_tasks_count > 0 {
                    ci.pending_tasks_count -= 1;
                }
            }
        }
        (task.task_arn.clone(), task.cluster_arn.clone())
    };
    s.push_event(LifecycleEvent {
        at: Utc::now(),
        event_type: "TaskStateChange".into(),
        task_arn: Some(arn),
        cluster_arn: Some(cluster_arn),
        last_status: Some("RUNNING".into()),
        detail: serde_json::json!({}),
    });
}

#[allow(clippy::too_many_arguments)]
fn finalize_stopped_multi(
    state: &SharedEcsState,
    account_id: &str,
    task_id: &str,
    final_containers: &[RunningContainer],
    primary_exit_code: i64,
    captured: &str,
    stop_code: &str,
    stopped_reason: Option<String>,
) {
    let mut accounts = state.write();
    let Some(s) = accounts.get_mut(account_id) else {
        return;
    };
    let (arn, cluster_arn) = {
        let Some(task) = s.tasks.get_mut(task_id) else {
            return;
        };
        task.last_status = "STOPPED".into();
        task.desired_status = "STOPPED".into();
        task.stopping_at = task.stopping_at.or(Some(Utc::now()));
        task.stopped_at = Some(Utc::now());
        task.stop_code = Some(stop_code.into());
        task.stopped_reason = stopped_reason.or(Some(format!("Exit code {}", primary_exit_code)));
        task.captured_logs = captured.to_string();
        for c in task.containers.iter_mut() {
            c.last_status = "STOPPED".into();
            if c.exit_code.is_none() {
                let mapped = final_containers
                    .iter()
                    .find(|r| r.name == c.name)
                    .and_then(|r| r.exit_code);
                c.exit_code = mapped.or(Some(primary_exit_code));
            }
        }
        if let Some(cluster) = s.clusters.get_mut(&task.cluster_name) {
            if cluster.running_tasks_count > 0 {
                cluster.running_tasks_count -= 1;
            }
        }
        if let Some(ref ci_arn) = task.container_instance_arn {
            if let Some(ci) = s
                .container_instances
                .values_mut()
                .find(|ci| ci.container_instance_arn == *ci_arn)
            {
                if ci.running_tasks_count > 0 {
                    ci.running_tasks_count -= 1;
                }
            }
        }
        (task.task_arn.clone(), task.cluster_arn.clone())
    };
    s.push_event(LifecycleEvent {
        at: Utc::now(),
        event_type: "TaskStateChange".into(),
        task_arn: Some(arn),
        cluster_arn: Some(cluster_arn),
        last_status: Some("STOPPED".into()),
        detail: serde_json::json!({
            "exitCode": primary_exit_code,
            "stopCode": stop_code,
        }),
    });
}

fn finalize_failure(state: &SharedEcsState, account_id: &str, task_id: &str, reason: &str) {
    let mut accounts = state.write();
    let Some(s) = accounts.get_mut(account_id) else {
        return;
    };
    let (arn, cluster_arn) = {
        let Some(task) = s.tasks.get_mut(task_id) else {
            return;
        };
        // Capture the prior status before we clobber it: if the task had
        // already reached RUNNING when execution failed (e.g. `docker wait`
        // blew up after the container started), we owe the cluster a
        // running-tasks decrement. Tasks that died before RUNNING only
        // ever incremented pendingTasksCount.
        let was_running = task.last_status == "RUNNING";
        task.last_status = "STOPPED".into();
        task.desired_status = "STOPPED".into();
        task.stopped_at = Some(Utc::now());
        task.stop_code = Some("TaskFailedToStart".into());
        task.stopped_reason = Some(reason.to_string());
        // Surface the failure reason on the /logs endpoint — without this,
        // a task that never reached RUNNING returns an empty log string,
        // leaving E2E assertions with no diagnostic.
        task.captured_logs = format!("[task failed to start]: {reason}");
        for c in task.containers.iter_mut() {
            c.last_status = "STOPPED".into();
            c.reason = Some(reason.to_string());
        }
        if let Some(cluster) = s.clusters.get_mut(&task.cluster_name) {
            if was_running {
                if cluster.running_tasks_count > 0 {
                    cluster.running_tasks_count -= 1;
                }
            } else if cluster.pending_tasks_count > 0 {
                cluster.pending_tasks_count -= 1;
            }
        }
        if let Some(ref ci_arn) = task.container_instance_arn {
            if let Some(ci) = s
                .container_instances
                .values_mut()
                .find(|ci| ci.container_instance_arn == *ci_arn)
            {
                if was_running {
                    if ci.running_tasks_count > 0 {
                        ci.running_tasks_count -= 1;
                    }
                } else if ci.pending_tasks_count > 0 {
                    ci.pending_tasks_count -= 1;
                }
            }
        }
        (task.task_arn.clone(), task.cluster_arn.clone())
    };
    s.push_event(LifecycleEvent {
        at: Utc::now(),
        event_type: "TaskFailedToStart".into(),
        task_arn: Some(arn),
        cluster_arn: Some(cluster_arn),
        last_status: Some("STOPPED".into()),
        detail: serde_json::json!({ "reason": reason }),
    });
}

/// Short helper for tests + snapshot code to sleep between state
/// transitions. Exposed on the crate boundary to keep test timing
/// centralized.
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EcsState, Task};
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    #[test]
    fn cli_available_for_known_missing_binary_is_false() {
        assert!(!fakecloud_core::container_net::cli_available(
            "definitely-not-a-real-cli-binary-xyz"
        ));
    }

    #[test]
    fn aws_ecr_uris_translate_for_local_pull() {
        assert_eq!(
            fakecloud_core::ecr_uri::translate_to_local(
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/app:latest",
                4566
            )
            .as_deref(),
            Some("127.0.0.1:4566/app:latest")
        );
    }

    fn make_task(task_id: &str) -> Task {
        Task {
            task_arn: Arn::new(
                "ecs",
                "us-east-1",
                "000000000000",
                &format!("task/default/{task_id}"),
            )
            .to_string(),
            task_id: task_id.into(),
            cluster_arn: "arn:aws:ecs:us-east-1:000000000000:cluster/default".into(),
            cluster_name: "default".into(),
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/app:1".into(),
            family: "app".into(),
            revision: 1,
            container_instance_arn: None,
            capacity_provider_name: None,
            last_status: "PENDING".into(),
            desired_status: "RUNNING".into(),
            launch_type: "FARGATE".into(),
            platform_version: None,
            cpu: None,
            memory: None,
            containers: Vec::new(),
            overrides: serde_json::json!({}),
            started_by: None,
            group: None,
            connectivity: "CONNECTING".into(),
            stop_code: None,
            stopped_reason: None,
            created_at: Utc::now(),
            started_at: None,
            stopping_at: None,
            stopped_at: None,
            pull_started_at: None,
            pull_stopped_at: None,
            connectivity_at: None,
            started_by_ref_id: None,
            execution_role_arn: None,
            task_role_arn: None,
            tags: Vec::new(),
            awslogs: None,
            captured_logs: String::new(),
            protection: None,
            enable_execute_command: false,
            attachments: Vec::new(),
            volume_configurations: Vec::new(),
            task_set_arn: None,
        }
    }

    #[test]
    fn finalize_failure_writes_reason_into_captured_logs() {
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create("000000000000");
        acct.tasks.insert("t1".into(), make_task("t1"));
        let state: SharedEcsState = Arc::new(RwLock::new(accounts));

        finalize_failure(
            &state,
            "000000000000",
            "t1",
            "failed to resolve secret DB_PASSWORD",
        );

        let accounts = state.read();
        let task = accounts
            .get("000000000000")
            .unwrap()
            .tasks
            .get("t1")
            .unwrap();
        assert_eq!(task.last_status, "STOPPED");
        assert_eq!(task.stop_code.as_deref(), Some("TaskFailedToStart"));
        assert!(
            task.captured_logs
                .contains("failed to resolve secret DB_PASSWORD"),
            "captured_logs missing reason: {:?}",
            task.captured_logs
        );
        assert!(
            task.captured_logs.starts_with("[task failed to start]:"),
            "captured_logs missing prefix: {:?}",
            task.captured_logs
        );
    }

    fn make_container(name: &str, essential: bool) -> crate::state::Container {
        crate::state::Container {
            container_arn: format!(
                "arn:aws:ecs:us-east-1:000000000000:container/default/abc/{name}"
            ),
            name: name.into(),
            image: "alpine".into(),
            task_arn: "arn:aws:ecs:us-east-1:000000000000:task/default/abc".into(),
            last_status: "RUNNING".into(),
            exit_code: None,
            reason: None,
            runtime_id: Some(format!("dockerid-{name}")),
            essential,
            cpu: None,
            memory: None,
            memory_reservation: None,
            network_bindings: Vec::new(),
            network_interfaces: Vec::new(),
            health_status: None,
            managed_agents: None,
            image_digest: None,
        }
    }

    #[test]
    fn task_should_stop_when_essential_exits() {
        let containers = vec![
            RunningContainer {
                name: "app".into(),
                container_id: "id-app".into(),
                essential: true,
                exit_code: Some(0),
                network_bindings: Vec::new(),
                image_digest: None,
            },
            RunningContainer {
                name: "sidecar".into(),
                container_id: "id-sc".into(),
                essential: false,
                exit_code: None,
                network_bindings: Vec::new(),
                image_digest: None,
            },
        ];
        assert!(task_should_stop(&containers));
    }

    #[test]
    fn task_keeps_running_when_only_non_essential_exits() {
        let containers = vec![
            RunningContainer {
                name: "app".into(),
                container_id: "id-app".into(),
                essential: true,
                exit_code: None,
                network_bindings: Vec::new(),
                image_digest: None,
            },
            RunningContainer {
                name: "sidecar".into(),
                container_id: "id-sc".into(),
                essential: false,
                exit_code: Some(0),
                network_bindings: Vec::new(),
                image_digest: None,
            },
        ];
        assert!(!task_should_stop(&containers));
    }

    #[test]
    fn task_stops_when_all_non_essentials_exit() {
        let containers = vec![
            RunningContainer {
                name: "a".into(),
                container_id: "id-a".into(),
                essential: false,
                exit_code: Some(0),
                network_bindings: Vec::new(),
                image_digest: None,
            },
            RunningContainer {
                name: "b".into(),
                container_id: "id-b".into(),
                essential: false,
                exit_code: Some(1),
                network_bindings: Vec::new(),
                image_digest: None,
            },
        ];
        assert!(task_should_stop(&containers));
    }

    #[test]
    fn finalize_stopped_multi_assigns_per_container_exit_codes() {
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create("000000000000");
        let mut t = make_task("t1");
        t.containers = vec![
            make_container("app", true),
            make_container("sidecar", false),
        ];
        acct.tasks.insert("t1".into(), t);
        let state: SharedEcsState = Arc::new(RwLock::new(accounts));

        let final_containers = vec![
            RunningContainer {
                name: "app".into(),
                container_id: "id-app".into(),
                essential: true,
                exit_code: Some(0),
                network_bindings: Vec::new(),
                image_digest: None,
            },
            RunningContainer {
                name: "sidecar".into(),
                container_id: "id-sc".into(),
                essential: false,
                exit_code: Some(137),
                network_bindings: Vec::new(),
                image_digest: None,
            },
        ];
        finalize_stopped_multi(
            &state,
            "000000000000",
            "t1",
            &final_containers,
            0,
            "captured",
            "EssentialContainerExited",
            None,
        );

        let accounts = state.read();
        let task = accounts
            .get("000000000000")
            .unwrap()
            .tasks
            .get("t1")
            .unwrap();
        assert_eq!(task.last_status, "STOPPED");
        assert_eq!(task.stop_code.as_deref(), Some("EssentialContainerExited"));
        let app = task.containers.iter().find(|c| c.name == "app").unwrap();
        let sc = task
            .containers
            .iter()
            .find(|c| c.name == "sidecar")
            .unwrap();
        assert_eq!(app.exit_code, Some(0));
        assert_eq!(sc.exit_code, Some(137));
        assert_eq!(app.last_status, "STOPPED");
        assert_eq!(sc.last_status, "STOPPED");
    }

    fn plan(name: &str, deps: &[&str]) -> ContainerPlan {
        ContainerPlan {
            container_name: name.into(),
            image: "alpine".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings: Vec::new(),
            network_mode: None,
            depends_on: deps
                .iter()
                .map(|s| DependsOn {
                    container_name: (*s).to_string(),
                    condition: DependsOnCondition::Start,
                })
                .collect(),
            health_check: None,
            volume_mounts: Vec::new(),
            ulimits: Vec::new(),
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        }
    }

    #[test]
    fn topo_sort_orders_by_depends_on() {
        // sidecar depends on app, so app must come first regardless of
        // declaration order.
        let plans = vec![plan("sidecar", &["app"]), plan("app", &[])];
        let ordered = topo_sort_plans(plans);
        assert_eq!(ordered[0].container_name, "app");
        assert_eq!(ordered[1].container_name, "sidecar");
    }

    #[test]
    fn topo_sort_preserves_declaration_order_when_no_deps() {
        let plans = vec![plan("first", &[]), plan("second", &[]), plan("third", &[])];
        let ordered = topo_sort_plans(plans);
        let names: Vec<&str> = ordered.iter().map(|p| p.container_name.as_str()).collect();
        assert_eq!(names, vec!["first", "second", "third"]);
    }

    #[test]
    fn topo_sort_handles_chain() {
        // c -> b -> a, declared in reverse so the topological sort must
        // bubble dependencies up.
        let plans = vec![plan("c", &["b"]), plan("b", &["a"]), plan("a", &[])];
        let ordered = topo_sort_plans(plans);
        let names: Vec<&str> = ordered.iter().map(|p| p.container_name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn topo_sort_ignores_unknown_dependency() {
        // depends_on names a container not in this task definition. Real
        // ECS would reject this at register time; we don't (yet), so the
        // unknown dep should just be skipped instead of stalling the sort.
        let plans = vec![plan("only", &["does-not-exist"])];
        let ordered = topo_sort_plans(plans);
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].container_name, "only");
    }

    #[test]
    fn topo_sort_recovers_from_cycle() {
        // Cyclic dependsOn: both plans should still appear in the output
        // so the runtime doesn't silently drop them.
        let plans = vec![plan("a", &["b"]), plan("b", &["a"])];
        let ordered = topo_sort_plans(plans);
        assert_eq!(ordered.len(), 2);
    }

    #[test]
    fn parse_health_check_fills_aws_defaults() {
        let v = serde_json::json!({
            "command": ["CMD-SHELL", "curl -f http://localhost/ || exit 1"],
        });
        let hc = __test_parse_health_check(&v).expect("parsed");
        assert_eq!(hc.command[0], "CMD-SHELL");
        assert_eq!(hc.interval_seconds, 30);
        assert_eq!(hc.timeout_seconds, 5);
        assert_eq!(hc.retries, 3);
        assert_eq!(hc.start_period_seconds, 0);
    }

    #[test]
    fn parse_health_check_overrides_explicit_values() {
        let v = serde_json::json!({
            "command": ["CMD", "/probe"],
            "interval": 7,
            "timeout": 2,
            "retries": 9,
            "startPeriod": 12,
        });
        let hc = __test_parse_health_check(&v).expect("parsed");
        assert_eq!(hc.interval_seconds, 7);
        assert_eq!(hc.timeout_seconds, 2);
        assert_eq!(hc.retries, 9);
        assert_eq!(hc.start_period_seconds, 12);
    }

    #[test]
    fn parse_health_check_returns_none_for_none_sentinel() {
        // ECS uses ["NONE"] to disable an inherited HEALTHCHECK; we
        // skip emission rather than passing a literal `none` to docker.
        let v = serde_json::json!({ "command": ["NONE"] });
        assert!(__test_parse_health_check(&v).is_none());
    }

    #[test]
    fn parse_health_check_returns_none_for_missing_command() {
        let v = serde_json::json!({ "interval": 30 });
        assert!(__test_parse_health_check(&v).is_none());
    }

    #[test]
    fn render_health_flags_emits_full_set_for_cmd_shell() {
        let hc = HealthCheckSpec {
            command: vec!["CMD-SHELL".into(), "curl -f http://localhost/".into()],
            interval_seconds: 15,
            timeout_seconds: 3,
            retries: 4,
            start_period_seconds: 10,
        };
        let flags = render_health_flags(&hc);
        assert_eq!(flags[0], "--health-cmd");
        assert_eq!(flags[1], "curl -f http://localhost/");
        assert!(flags.contains(&"--health-interval=15s".to_string()));
        assert!(flags.contains(&"--health-timeout=3s".to_string()));
        assert!(flags.contains(&"--health-retries=4".to_string()));
        assert!(flags.contains(&"--health-start-period=10s".to_string()));
    }

    #[test]
    fn render_health_flags_joins_cmd_argv_with_spaces() {
        // CMD form in ECS is argv-style; docker `--health-cmd` only
        // accepts a single shell string, so we collapse with spaces.
        let hc = HealthCheckSpec {
            command: vec![
                "CMD".into(),
                "/bin/probe".into(),
                "--port".into(),
                "8080".into(),
            ],
            interval_seconds: 30,
            timeout_seconds: 5,
            retries: 3,
            start_period_seconds: 0,
        };
        let flags = render_health_flags(&hc);
        assert_eq!(flags[1], "/bin/probe --port 8080");
    }

    #[test]
    fn build_run_argv_emits_health_flags_when_present() {
        let plan = ContainerPlan {
            container_name: "app".into(),
            image: "alpine".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings: Vec::new(),
            network_mode: None,
            depends_on: Vec::new(),
            health_check: Some(HealthCheckSpec {
                command: vec!["CMD-SHELL".into(), "true".into()],
                interval_seconds: 5,
                timeout_seconds: 2,
                retries: 1,
                start_period_seconds: 1,
            }),
            volume_mounts: Vec::new(),
            ulimits: Vec::new(),
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        };
        let argv = build_run_argv(
            &plan,
            &[],
            "task-1",
            "host.docker.internal",
            None,
            "alpine",
            true,
        );
        let joined = argv.join(" ");
        assert!(joined.contains("--health-cmd true"), "argv: {joined}");
        assert!(joined.contains("--health-interval=5s"), "argv: {joined}");
        assert!(joined.contains("--health-timeout=2s"), "argv: {joined}");
        assert!(joined.contains("--health-retries=1"), "argv: {joined}");
        assert!(
            joined.contains("--health-start-period=1s"),
            "argv: {joined}"
        );
    }

    #[test]
    fn build_run_argv_emits_no_health_flags_when_absent() {
        let plan = ContainerPlan {
            container_name: "app".into(),
            image: "alpine".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings: Vec::new(),
            network_mode: None,
            depends_on: Vec::new(),
            health_check: None,
            volume_mounts: Vec::new(),
            ulimits: Vec::new(),
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        };
        let argv = build_run_argv(
            &plan,
            &[],
            "task-1",
            "host.docker.internal",
            None,
            "alpine",
            true,
        );
        assert!(!argv.iter().any(|s| s.starts_with("--health")));
    }

    #[test]
    fn docker_health_to_ecs_maps_known_states() {
        assert_eq!(docker_health_to_ecs("healthy"), "HEALTHY");
        assert_eq!(docker_health_to_ecs("HEALTHY"), "HEALTHY");
        assert_eq!(docker_health_to_ecs("unhealthy"), "UNHEALTHY");
        assert_eq!(docker_health_to_ecs("starting"), "UNKNOWN");
        assert_eq!(docker_health_to_ecs("none"), "UNKNOWN");
        assert_eq!(docker_health_to_ecs(""), "UNKNOWN");
    }

    /// `host.sourcePath` becomes a host bind mount with the path
    /// passed straight through to docker.
    #[test]
    fn resolve_host_bind_volume_uses_source_path() {
        let mut volumes = std::collections::HashMap::new();
        let v = serde_json::json!({
            "name": "data",
            "host": { "sourcePath": "/var/lib/myapp" }
        });
        volumes.insert("data".to_string(), &v);
        let mp = serde_json::json!({
            "sourceVolume": "data",
            "containerPath": "/app/data",
            "readOnly": false
        });
        let resolved = resolve_mount_point(&mp, &volumes).expect("resolved");
        assert_eq!(resolved.source, "/var/lib/myapp");
        assert_eq!(resolved.container_path, "/app/data");
        assert!(!resolved.read_only);
    }

    /// `readOnly: true` on the mount point appends `:ro` to the
    /// rendered docker `-v` flag.
    #[test]
    fn read_only_mount_renders_ro_suffix() {
        let plan = ContainerPlan {
            container_name: "app".into(),
            image: "alpine".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings: Vec::new(),
            network_mode: None,
            depends_on: Vec::new(),
            health_check: None,
            volume_mounts: vec![VolumeMount {
                source: "/host/path".into(),
                container_path: "/in/container".into(),
                read_only: true,
            }],
            ulimits: Vec::new(),
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        };
        let argv = build_run_argv(
            &plan,
            &[],
            "task-1",
            "host.docker.internal",
            None,
            "alpine",
            true,
        );
        let pair = argv
            .windows(2)
            .find(|w| w[0] == "-v")
            .expect("expected -v flag");
        assert_eq!(pair[1], "/host/path:/in/container:ro");
    }

    /// EFS volumes resolve to a stub directory under `/tmp/fakecloud/efs`
    /// keyed by `fileSystemId`. `rootDirectory` (when set and not `/`)
    /// is appended so different mount targets within the same
    /// filesystem stay isolated.
    #[test]
    fn resolve_efs_volume_uses_stub_dir() {
        let mut volumes = std::collections::HashMap::new();
        let v = serde_json::json!({
            "name": "efs-vol",
            "efsVolumeConfiguration": {
                "fileSystemId": "fs-12345678",
                "rootDirectory": "/exports/app"
            }
        });
        volumes.insert("efs-vol".to_string(), &v);
        let mp = serde_json::json!({
            "sourceVolume": "efs-vol",
            "containerPath": "/mnt/efs"
        });
        let resolved = resolve_mount_point(&mp, &volumes).expect("resolved");
        assert_eq!(
            resolved.source,
            "/tmp/fakecloud/efs/fs-12345678/exports/app"
        );
        assert_eq!(resolved.container_path, "/mnt/efs");
    }

    /// EFS without `rootDirectory` (or with `/`) maps to the root of
    /// the filesystem stub so multiple tasks targeting the same id
    /// share state.
    #[test]
    fn efs_without_root_directory_uses_filesystem_root() {
        assert_eq!(
            stub_dir_for("efs", "fs-abc", "/"),
            "/tmp/fakecloud/efs/fs-abc"
        );
        assert_eq!(
            stub_dir_for("efs", "fs-abc", ""),
            "/tmp/fakecloud/efs/fs-abc"
        );
    }

    /// `dockerVolumeConfiguration` resolves to the volume name itself,
    /// which docker treats as a named volume reference. No host path
    /// is materialised — docker creates the volume on first reference.
    #[test]
    fn resolve_docker_named_volume_uses_volume_name() {
        let mut volumes = std::collections::HashMap::new();
        let v = serde_json::json!({
            "name": "named-vol",
            "dockerVolumeConfiguration": {
                "scope": "task",
                "driver": "local"
            }
        });
        volumes.insert("named-vol".to_string(), &v);
        let mp = serde_json::json!({
            "sourceVolume": "named-vol",
            "containerPath": "/data"
        });
        let resolved = resolve_mount_point(&mp, &volumes).expect("resolved");
        assert_eq!(resolved.source, "named-vol");
        assert_eq!(resolved.container_path, "/data");
    }

    /// FSx for Windows uses the same stub-directory pattern as EFS but
    /// scoped under `/tmp/fakecloud/fsx/<filesystemId>/`.
    #[test]
    fn resolve_fsx_volume_uses_stub_dir() {
        let mut volumes = std::collections::HashMap::new();
        let v = serde_json::json!({
            "name": "fsx-vol",
            "fsxWindowsFileServerVolumeConfiguration": {
                "fileSystemId": "fs-xyz",
                "rootDirectory": "share"
            }
        });
        volumes.insert("fsx-vol".to_string(), &v);
        let mp = serde_json::json!({
            "sourceVolume": "fsx-vol",
            "containerPath": "C:\\data"
        });
        let resolved = resolve_mount_point(&mp, &volumes).expect("resolved");
        assert_eq!(resolved.source, "/tmp/fakecloud/fsx/fs-xyz/share");
    }

    /// Mount points that reference an undeclared `sourceVolume` resolve
    /// to `None` so `build_container_plans` skips them rather than
    /// emitting a broken `-v` flag.
    #[test]
    fn unknown_source_volume_returns_none() {
        let volumes = std::collections::HashMap::new();
        let mp = serde_json::json!({
            "sourceVolume": "missing",
            "containerPath": "/x"
        });
        assert!(resolve_mount_point(&mp, &volumes).is_none());
    }

    /// `find_depends_on_cycle` returns the back-edge endpoints when a
    /// trivial 2-cycle exists. Real ECS would reject this at register
    /// time; our service-level handler relies on this helper.
    #[test]
    fn find_depends_on_cycle_detects_two_node_cycle() {
        let cds = vec![
            serde_json::json!({
                "name": "a",
                "image": "alpine",
                "dependsOn": [{"containerName": "b", "condition": "START"}],
            }),
            serde_json::json!({
                "name": "b",
                "image": "alpine",
                "dependsOn": [{"containerName": "a", "condition": "START"}],
            }),
        ];
        let cycle = find_depends_on_cycle(&cds);
        assert!(cycle.is_some(), "expected cycle to be detected");
    }

    /// A three-node chain (a -> b -> c) is acyclic and must not be
    /// flagged. Guards against an over-eager DFS reporting back-edges
    /// from already-finished nodes.
    #[test]
    fn find_depends_on_cycle_accepts_chain() {
        let cds = vec![
            serde_json::json!({
                "name": "a",
                "image": "alpine",
                "dependsOn": [{"containerName": "b", "condition": "START"}],
            }),
            serde_json::json!({
                "name": "b",
                "image": "alpine",
                "dependsOn": [{"containerName": "c", "condition": "START"}],
            }),
            serde_json::json!({
                "name": "c",
                "image": "alpine",
            }),
        ];
        assert!(find_depends_on_cycle(&cds).is_none());
    }

    /// `dependsOn[]` entries that name a container outside the task
    /// definition are ignored by the cycle check (they can't form a
    /// cycle by definition; runtime also drops them).
    #[test]
    fn find_depends_on_cycle_ignores_unknown_target() {
        let cds = vec![serde_json::json!({
            "name": "only",
            "image": "alpine",
            "dependsOn": [{"containerName": "ghost", "condition": "START"}],
        })];
        assert!(find_depends_on_cycle(&cds).is_none());
    }

    /// `condition_is_met` covers each AWS condition value against a
    /// simulated docker inspect snapshot. Pinning these mappings here
    /// catches accidental re-orderings of the match arms.
    #[test]
    fn condition_is_met_matches_aws_semantics() {
        let running = InspectedState {
            started: true,
            exited: false,
            exit_code: 0,
            health: None,
        };
        let exited_ok = InspectedState {
            started: true,
            exited: true,
            exit_code: 0,
            health: None,
        };
        let exited_fail = InspectedState {
            started: true,
            exited: true,
            exit_code: 1,
            health: None,
        };
        let healthy = InspectedState {
            started: true,
            exited: false,
            exit_code: 0,
            health: Some("healthy".into()),
        };

        // START is satisfied as soon as the container has started, even
        // if it later exited.
        assert!(condition_is_met(DependsOnCondition::Start, &running));
        assert!(condition_is_met(DependsOnCondition::Start, &exited_ok));

        // COMPLETE requires an exit, regardless of code.
        assert!(!condition_is_met(DependsOnCondition::Complete, &running));
        assert!(condition_is_met(DependsOnCondition::Complete, &exited_ok));
        assert!(condition_is_met(DependsOnCondition::Complete, &exited_fail));

        // SUCCESS requires an exit AND code 0.
        assert!(!condition_is_met(DependsOnCondition::Success, &running));
        assert!(condition_is_met(DependsOnCondition::Success, &exited_ok));
        assert!(!condition_is_met(DependsOnCondition::Success, &exited_fail));

        // HEALTHY requires Health.Status == "healthy".
        assert!(!condition_is_met(DependsOnCondition::Healthy, &running));
        assert!(condition_is_met(DependsOnCondition::Healthy, &healthy));
    }

    /// `DependsOnCondition::parse` accepts the four AWS-spelled values
    /// and rejects everything else — register-time validation depends on
    /// this returning `None` for unknowns.
    #[test]
    fn depends_on_condition_parse_round_trips() {
        assert_eq!(
            DependsOnCondition::parse("START"),
            Some(DependsOnCondition::Start)
        );
        assert_eq!(
            DependsOnCondition::parse("COMPLETE"),
            Some(DependsOnCondition::Complete)
        );
        assert_eq!(
            DependsOnCondition::parse("SUCCESS"),
            Some(DependsOnCondition::Success)
        );
        assert_eq!(
            DependsOnCondition::parse("HEALTHY"),
            Some(DependsOnCondition::Healthy)
        );
        assert_eq!(DependsOnCondition::parse("start"), None);
        assert_eq!(DependsOnCondition::parse("ANY"), None);
    }

    // ── ulimits + linuxParameters + misc docker flags (O6) ──

    #[test]
    fn build_run_argv_emits_ulimits() {
        let plan = ContainerPlan {
            container_name: "app".into(),
            image: "alpine".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings: Vec::new(),
            network_mode: None,
            depends_on: Vec::new(),
            health_check: None,
            volume_mounts: Vec::new(),
            ulimits: vec![Ulimit {
                name: "nofile".into(),
                soft_limit: 1024,
                hard_limit: 2048,
            }],
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        };
        let argv = build_run_argv(&plan, &[], "t", "host.docker.internal", None, "img", true);
        assert!(argv.contains(&"--ulimit".to_string()));
        assert!(argv.contains(&"nofile=1024:2048".to_string()));
    }

    #[test]
    fn build_run_argv_emits_linux_parameters() {
        let plan = ContainerPlan {
            container_name: "app".into(),
            image: "alpine".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings: Vec::new(),
            network_mode: None,
            depends_on: Vec::new(),
            health_check: None,
            volume_mounts: Vec::new(),
            ulimits: Vec::new(),
            linux_parameters: Some(LinuxParameters {
                capabilities_add: vec!["NET_ADMIN".into()],
                capabilities_drop: vec!["ALL".into()],
                devices: vec![Device {
                    host_path: "/dev/zero".into(),
                    container_path: "/dev/zero".into(),
                    permissions: "rwm".into(),
                }],
                init_process_enabled: true,
                shared_memory_size: Some(256),
                sysctls: vec![Sysctl {
                    name: "net.ipv4.ip_forward".into(),
                    value: "1".into(),
                }],
                tmpfs: vec![Tmpfs {
                    container_path: "/tmp".into(),
                    size: 128,
                    mount_options: vec!["noexec".into()],
                }],
                privileged: true,
            }),
            stop_timeout: Some(30),
            user: Some("1000:1000".into()),
            working_directory: Some("/app".into()),
            tty: true,
            interactive: true,
            readonly_rootfs: true,
        };
        let argv = build_run_argv(&plan, &[], "t", "host.docker.internal", None, "img", true);
        assert!(argv.contains(&"--cap-add".to_string()));
        assert!(argv.contains(&"NET_ADMIN".to_string()));
        assert!(argv.contains(&"--cap-drop".to_string()));
        assert!(argv.contains(&"ALL".to_string()));
        assert!(argv.contains(&"--device".to_string()));
        assert!(argv.contains(&"/dev/zero:/dev/zerorwm".to_string()));
        assert!(argv.contains(&"--init".to_string()));
        assert!(argv.contains(&"--shm-size".to_string()));
        assert!(argv.contains(&"256m".to_string()));
        assert!(argv.contains(&"--sysctl".to_string()));
        assert!(argv.contains(&"net.ipv4.ip_forward=1".to_string()));
        assert!(argv.contains(&"--tmpfs".to_string()));
        assert!(argv.contains(&"--privileged".to_string()));
        assert!(argv.contains(&"--stop-timeout".to_string()));
        assert!(argv.contains(&"30".to_string()));
        assert!(argv.contains(&"--user".to_string()));
        assert!(argv.contains(&"1000:1000".to_string()));
        assert!(argv.contains(&"--workdir".to_string()));
        assert!(argv.contains(&"/app".to_string()));
        assert!(argv.contains(&"--tty".to_string()));
        assert!(argv.contains(&"--interactive".to_string()));
        assert!(argv.contains(&"--read-only".to_string()));
    }

    #[test]
    fn parse_linux_parameters_fills_defaults() {
        let raw = serde_json::json!({"initProcessEnabled": true});
        let lp = parse_linux_parameters(&raw).expect("parses");
        assert!(lp.init_process_enabled);
        assert!(!lp.privileged);
        assert!(lp.capabilities_add.is_empty());
    }

    #[test]
    fn parse_device_uses_default_permissions() {
        let raw = serde_json::json!({"hostPath": "/dev/null", "containerPath": "/dev/null"});
        let dev = parse_device(&raw).expect("parses");
        assert_eq!(dev.permissions, "rwm");
    }

    #[test]
    fn compute_elbv2_targets_empty_when_no_group() {
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create("000000000000");
        let mut task = make_task("t1");
        task.group = None;
        acct.tasks.insert("t1".into(), task);
        let state = acct.clone();
        let targets = compute_elbv2_targets(&state, state.tasks.get("t1").unwrap());
        assert!(targets.is_empty());
    }

    #[test]
    fn compute_elbv2_targets_bridge_mode_uses_localhost_and_host_port() {
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create("000000000000");

        let td = crate::state::TaskDefinition {
            family: "app".into(),
            revision: 1,
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/app:1".into(),
            container_definitions: Vec::new(),
            network_mode: Some("bridge".into()),
            status: "ACTIVE".into(),
            task_role_arn: None,
            execution_role_arn: None,
            requires_compatibilities: Vec::new(),
            compatibilities: Vec::new(),
            cpu: None,
            memory: None,
            pid_mode: None,
            ipc_mode: None,
            volumes: Vec::new(),
            placement_constraints: Vec::new(),
            proxy_configuration: None,
            inference_accelerators: Vec::new(),
            ephemeral_storage: None,
            runtime_platform: None,
            requires_attributes: Vec::new(),
            registered_at: Utc::now(),
            registered_by: None,
            deregistered_at: None,
            tags: Vec::new(),
            enable_fault_injection: None,
        };
        acct.task_definitions.insert("app".into(), {
            let mut m = std::collections::BTreeMap::new();
            m.insert(1, td);
            m
        });

        let service = crate::state::Service {
            service_name: "svc".into(),
            service_arn: "arn:aws:ecs:us-east-1:000000000000:service/default/svc".into(),
            cluster_name: "default".into(),
            cluster_arn: "arn:aws:ecs:us-east-1:000000000000:cluster/default".into(),
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/app:1".into(),
            family: "app".into(),
            revision: 1,
            desired_count: 1,
            running_count: 0,
            pending_count: 0,
            launch_type: "FARGATE".into(),
            status: "ACTIVE".into(),
            scheduling_strategy: "REPLICA".into(),
            deployment_controller: "ECS".into(),
            minimum_healthy_percent: Some(0),
            maximum_percent: Some(200),
            circuit_breaker: None,
            deployments: Vec::new(),
            load_balancers: vec![serde_json::json!({
                "targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg/abc",
                "containerName": "app",
                "containerPort": 80,
            })],
            service_registries: Vec::new(),
            placement_constraints: Vec::new(),
            placement_strategy: Vec::new(),
            network_configuration: None,
            volume_configurations: vec![],
            tags: Vec::new(),
            created_at: Utc::now(),
            created_by: None,
            role_arn: None,
            platform_version: None,
            health_check_grace_period_seconds: None,
            enable_execute_command: false,
            enable_ecs_managed_tags: false,
            propagate_tags: None,
            capacity_provider_strategy: Vec::new(),
            availability_zone_rebalancing: None,
        };
        acct.services.insert(
            crate::state::EcsState::service_key("default", "svc"),
            service,
        );

        let mut task = make_task("t1");
        task.group = Some("service:svc".into());
        task.containers = vec![crate::state::Container {
            container_arn: "arn:aws:ecs:us-east-1:000000000000:container/default/abc/app".into(),
            name: "app".into(),
            image: "alpine".into(),
            task_arn: task.task_arn.clone(),
            last_status: "RUNNING".into(),
            exit_code: None,
            reason: None,
            runtime_id: Some("dockerid-app".into()),
            essential: true,
            cpu: None,
            memory: None,
            memory_reservation: None,
            network_bindings: vec![serde_json::json!({
                "bindIP": "0.0.0.0",
                "containerPort": 80,
                "hostPort": 32768,
                "protocol": "tcp",
            })],
            network_interfaces: Vec::new(),
            health_status: None,
            managed_agents: None,
            image_digest: None,
        }];
        acct.tasks.insert("t1".into(), task);

        let state = acct.clone();
        let targets = compute_elbv2_targets(&state, state.tasks.get("t1").unwrap());
        assert_eq!(targets.len(), 1);
        let (arn, tg_targets) = &targets[0];
        assert_eq!(
            arn,
            "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg/abc"
        );
        assert_eq!(tg_targets.len(), 1);
        assert_eq!(tg_targets[0].0, "127.0.0.1");
        assert_eq!(tg_targets[0].1, Some(32768));
    }

    #[test]
    fn compute_elbv2_targets_awsvpc_uses_eni_ip() {
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create("000000000000");

        let td = crate::state::TaskDefinition {
            family: "app".into(),
            revision: 1,
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/app:1".into(),
            container_definitions: Vec::new(),
            network_mode: Some("awsvpc".into()),
            status: "ACTIVE".into(),
            task_role_arn: None,
            execution_role_arn: None,
            requires_compatibilities: Vec::new(),
            compatibilities: Vec::new(),
            cpu: None,
            memory: None,
            pid_mode: None,
            ipc_mode: None,
            volumes: Vec::new(),
            placement_constraints: Vec::new(),
            proxy_configuration: None,
            inference_accelerators: Vec::new(),
            ephemeral_storage: None,
            runtime_platform: None,
            requires_attributes: Vec::new(),
            registered_at: Utc::now(),
            registered_by: None,
            deregistered_at: None,
            tags: Vec::new(),
            enable_fault_injection: None,
        };
        acct.task_definitions.insert("app".into(), {
            let mut m = std::collections::BTreeMap::new();
            m.insert(1, td);
            m
        });

        let service = crate::state::Service {
            service_name: "svc".into(),
            service_arn: "arn:aws:ecs:us-east-1:000000000000:service/default/svc".into(),
            cluster_name: "default".into(),
            cluster_arn: "arn:aws:ecs:us-east-1:000000000000:cluster/default".into(),
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/app:1".into(),
            family: "app".into(),
            revision: 1,
            desired_count: 1,
            running_count: 0,
            pending_count: 0,
            launch_type: "FARGATE".into(),
            status: "ACTIVE".into(),
            scheduling_strategy: "REPLICA".into(),
            deployment_controller: "ECS".into(),
            minimum_healthy_percent: Some(0),
            maximum_percent: Some(200),
            circuit_breaker: None,
            deployments: Vec::new(),
            load_balancers: vec![serde_json::json!({
                "targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg/abc",
                "containerName": "app",
                "containerPort": 80,
            })],
            service_registries: Vec::new(),
            placement_constraints: Vec::new(),
            placement_strategy: Vec::new(),
            network_configuration: None,
            volume_configurations: vec![],
            tags: Vec::new(),
            created_at: Utc::now(),
            created_by: None,
            role_arn: None,
            platform_version: None,
            health_check_grace_period_seconds: None,
            enable_execute_command: false,
            enable_ecs_managed_tags: false,
            propagate_tags: None,
            capacity_provider_strategy: Vec::new(),
            availability_zone_rebalancing: None,
        };
        acct.services.insert(
            crate::state::EcsState::service_key("default", "svc"),
            service,
        );

        let mut task = make_task("t1");
        task.group = Some("service:svc".into());
        task.attachments = vec![crate::state::TaskAttachment {
            id: "eni-123".into(),
            attachment_type: "eni".into(),
            status: "ATTACHED".into(),
            details: vec![
                crate::state::AttachmentDetail {
                    name: "privateIPv4Address".into(),
                    value: "172.18.0.2".into(),
                },
                crate::state::AttachmentDetail {
                    name: "macAddress".into(),
                    value: "02:42:ac:12:00:02".into(),
                },
            ],
        }];
        acct.tasks.insert("t1".into(), task);

        let state = acct.clone();
        let targets = compute_elbv2_targets(&state, state.tasks.get("t1").unwrap());
        assert_eq!(targets.len(), 1);
        let (arn, tg_targets) = &targets[0];
        assert_eq!(
            arn,
            "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg/abc"
        );
        assert_eq!(tg_targets.len(), 1);
        assert_eq!(tg_targets[0].0, "172.18.0.2");
        assert_eq!(tg_targets[0].1, Some(80));
    }
}
