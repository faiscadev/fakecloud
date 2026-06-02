use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedEcsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<EcsState>>>;

impl fakecloud_core::multi_account::AccountState for EcsState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const ECS_SNAPSHOT_SCHEMA_VERSION: u32 = 4;

/// Top-level persisted ECS snapshot. Mirrors the multi-account snapshot
/// convention used by Kinesis/ECR/ElastiCache so `main.rs` can share the
/// load/save pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EcsSnapshot {
    pub schema_version: u32,
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<EcsState>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EcsState {
    pub account_id: String,
    pub region: String,
    /// Cluster state keyed by cluster name.
    pub clusters: BTreeMap<String, Cluster>,
    /// Task definitions keyed by `family` -> `revision` -> definition.
    /// ECS revisions monotonically increase per-family regardless of
    /// deregistration, so we track the running counter separately.
    pub task_definitions: BTreeMap<String, BTreeMap<i32, TaskDefinition>>,
    /// Running revision counter per family. Grows monotonically even
    /// after task definitions are deregistered or deleted.
    pub next_revision: BTreeMap<String, i32>,
    /// Account-default settings (PutAccountSettingDefault). Keyed by
    /// setting name (e.g. `serviceLongArnFormat`).
    pub account_setting_defaults: BTreeMap<String, String>,
    /// Per-principal account settings (PutAccountSetting). Keyed by
    /// principal ARN, then setting name.
    pub principal_account_settings: BTreeMap<String, BTreeMap<String, String>>,
    /// Tasks keyed by task ID (the trailing segment of the task ARN).
    #[serde(default)]
    pub tasks: BTreeMap<String, Task>,
    /// Lifecycle event log for introspection. Bounded at 1024 entries
    /// (oldest dropped) so long-running servers don't grow unboundedly.
    #[serde(default)]
    pub events: Vec<LifecycleEvent>,
    /// Services keyed by service name within an account. ECS requires
    /// unique service names per cluster, and since service names are
    /// already unique per-cluster globally we scope keys by
    /// `cluster_name:service_name` in [`EcsState::service_key`].
    #[serde(default)]
    pub services: BTreeMap<String, Service>,
    /// Container instances keyed by `cluster/arn-suffix`. Users register
    /// EC2 hosts here; fakecloud still runs tasks via Docker regardless,
    /// but the control-plane records remain so `DescribeContainerInstances`
    /// round-trips.
    #[serde(default)]
    pub container_instances: BTreeMap<String, ContainerInstance>,
    /// Custom attributes keyed by `cluster/target-arn-or-id/name`.
    #[serde(default)]
    pub attributes: BTreeMap<String, Attribute>,
    /// Capacity providers keyed by name.
    #[serde(default)]
    pub capacity_providers: BTreeMap<String, CapacityProvider>,
    /// Task sets keyed by `cluster/service/task-set-id`.
    #[serde(default)]
    pub task_sets: BTreeMap<String, TaskSet>,
    /// Daemon task definitions keyed by `family` -> `revision` -> definition.
    /// Same shape as `task_definitions` but isolated since daemon defs use
    /// the dedicated `RegisterDaemonTaskDefinition` op and have their own
    /// revision counter.
    #[serde(default)]
    pub daemon_task_definitions: BTreeMap<String, BTreeMap<i32, DaemonTaskDefinition>>,
    /// Per-family monotonic revision counter for daemon task defs.
    #[serde(default)]
    pub next_daemon_revision: BTreeMap<String, i32>,
    /// Daemons keyed by `cluster/daemon-name`. Daemons are cluster-scoped
    /// and run one task per matching capacity provider per AWS spec.
    #[serde(default)]
    pub daemons: BTreeMap<String, Daemon>,
    /// Daemon deployment history keyed by deployment ARN. Each
    /// CreateDaemon / UpdateDaemon mints a new deployment record.
    #[serde(default)]
    pub daemon_deployments: BTreeMap<String, DaemonDeployment>,
    /// Express Gateway services keyed by `cluster/service-name`. The
    /// 2026 Express Gateway feature is a serverless container service
    /// with built-in load balancing and autoscaling.
    #[serde(default)]
    pub express_gateway_services: BTreeMap<String, ExpressGatewayService>,
}

impl EcsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            clusters: BTreeMap::new(),
            task_definitions: BTreeMap::new(),
            next_revision: BTreeMap::new(),
            account_setting_defaults: BTreeMap::new(),
            principal_account_settings: BTreeMap::new(),
            tasks: BTreeMap::new(),
            events: Vec::new(),
            services: BTreeMap::new(),
            container_instances: BTreeMap::new(),
            attributes: BTreeMap::new(),
            capacity_providers: BTreeMap::new(),
            task_sets: BTreeMap::new(),
            daemon_task_definitions: BTreeMap::new(),
            next_daemon_revision: BTreeMap::new(),
            daemons: BTreeMap::new(),
            daemon_deployments: BTreeMap::new(),
            express_gateway_services: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.clusters.clear();
        self.task_definitions.clear();
        self.next_revision.clear();
        self.account_setting_defaults.clear();
        self.principal_account_settings.clear();
        self.tasks.clear();
        self.events.clear();
        self.services.clear();
        self.container_instances.clear();
        self.attributes.clear();
        self.capacity_providers.clear();
        self.task_sets.clear();
        self.daemon_task_definitions.clear();
        self.next_daemon_revision.clear();
        self.daemons.clear();
        self.daemon_deployments.clear();
        self.express_gateway_services.clear();
    }

    /// Services are uniquely identified by `(cluster, name)` within an
    /// account; this helper composes the storage key used in
    /// `self.services`.
    pub fn service_key(cluster_name: &str, service_name: &str) -> String {
        format!("{}/{}", cluster_name, service_name)
    }

    pub fn service_arn(&self, cluster_name: &str, service_name: &str) -> String {
        if self.arn_format_disabled("serviceLongArnFormat") {
            // Pre-Nov-2018 short form: no cluster segment.
            format!(
                "arn:aws:ecs:{}:{}:service/{}",
                self.region, self.account_id, service_name
            )
        } else {
            format!(
                "arn:aws:ecs:{}:{}:service/{}/{}",
                self.region, self.account_id, cluster_name, service_name
            )
        }
    }

    pub fn task_arn(&self, cluster_name: &str, task_id: &str) -> String {
        if self.arn_format_disabled("taskLongArnFormat") {
            format!(
                "arn:aws:ecs:{}:{}:task/{}",
                self.region, self.account_id, task_id
            )
        } else {
            format!(
                "arn:aws:ecs:{}:{}:task/{}/{}",
                self.region, self.account_id, cluster_name, task_id
            )
        }
    }

    pub fn container_instance_arn(&self, cluster_name: &str, instance_id: &str) -> String {
        if self.arn_format_disabled("containerInstanceLongArnFormat") {
            format!(
                "arn:aws:ecs:{}:{}:container-instance/{}",
                self.region, self.account_id, instance_id
            )
        } else {
            format!(
                "arn:aws:ecs:{}:{}:container-instance/{}/{}",
                self.region, self.account_id, cluster_name, instance_id
            )
        }
    }

    /// Resolve the effective value of an account setting. Principal
    /// overrides win over account-level defaults, matching AWS's
    /// PutAccountSetting / PutAccountSettingDefault layering. With no
    /// `principal_arn` argument the caller gets the account default.
    pub fn effective_account_setting(
        &self,
        name: &str,
        principal_arn: Option<&str>,
    ) -> Option<String> {
        if let Some(arn) = principal_arn {
            if let Some(p) = self.principal_account_settings.get(arn) {
                if let Some(v) = p.get(name) {
                    return Some(v.clone());
                }
            }
        }
        self.account_setting_defaults.get(name).cloned()
    }

    /// `true` when the given `*LongArnFormat` setting has been set to
    /// `disabled`. The default (including unset) is long format —
    /// matches AWS's current behaviour where long ARNs are mandatory
    /// since Jan 2020 but the settings still flip for backward-compat.
    fn arn_format_disabled(&self, setting_name: &str) -> bool {
        matches!(
            self.effective_account_setting(setting_name, None)
                .as_deref(),
            Some("disabled")
        )
    }

    /// Append a lifecycle event, trimming the oldest when the cap is hit.
    pub fn push_event(&mut self, event: LifecycleEvent) {
        const MAX_EVENTS: usize = 1024;
        if self.events.len() >= MAX_EVENTS {
            self.events.drain(0..self.events.len() - MAX_EVENTS + 1);
        }
        self.events.push(event);
    }

    pub fn cluster_arn(&self, cluster_name: &str) -> String {
        format!(
            "arn:aws:ecs:{}:{}:cluster/{}",
            self.region, self.account_id, cluster_name
        )
    }

    pub fn task_definition_arn(&self, family: &str, revision: i32) -> String {
        format!(
            "arn:aws:ecs:{}:{}:task-definition/{}:{}",
            self.region, self.account_id, family, revision
        )
    }

    /// Given a user-supplied cluster reference (name or ARN), return the
    /// cluster name. Defaults to `"default"` when `None`/empty, matching
    /// the AWS CLI behaviour.
    pub fn resolve_cluster_name(input: Option<&str>) -> String {
        let raw = input.unwrap_or("").trim();
        if raw.is_empty() {
            return "default".to_string();
        }
        if let Some(name) = raw.rsplit_once('/').map(|(_, n)| n) {
            return name.to_string();
        }
        raw.to_string()
    }

    /// Bump and return the next revision number for a family. Never
    /// reused: monotonically increases even across deregistration.
    pub fn allocate_revision(&mut self, family: &str) -> i32 {
        let next = self.next_revision.entry(family.to_string()).or_insert(0);
        *next += 1;
        *next
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cluster {
    pub cluster_name: String,
    pub cluster_arn: String,
    pub status: String,
    pub registered_container_instances_count: i32,
    pub running_tasks_count: i32,
    pub pending_tasks_count: i32,
    pub active_services_count: i32,
    #[serde(default)]
    pub statistics: Vec<Value>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    #[serde(default)]
    pub settings: Vec<Value>,
    pub configuration: Option<Value>,
    #[serde(default)]
    pub capacity_providers: Vec<String>,
    #[serde(default)]
    pub default_capacity_provider_strategy: Vec<Value>,
    #[serde(default)]
    pub attachments: Vec<Value>,
    pub attachments_status: Option<String>,
    pub service_connect_defaults: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl Cluster {
    pub fn new(cluster_name: &str, cluster_arn: String) -> Self {
        Self {
            cluster_name: cluster_name.to_string(),
            cluster_arn,
            status: "ACTIVE".to_string(),
            registered_container_instances_count: 0,
            running_tasks_count: 0,
            pending_tasks_count: 0,
            active_services_count: 0,
            statistics: Vec::new(),
            tags: Vec::new(),
            settings: Vec::new(),
            configuration: None,
            capacity_providers: Vec::new(),
            default_capacity_provider_strategy: Vec::new(),
            attachments: Vec::new(),
            attachments_status: None,
            service_connect_defaults: None,
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TagEntry {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskDefinition {
    pub family: String,
    pub revision: i32,
    pub task_definition_arn: String,
    /// Free-form container definitions preserved as the JSON the caller
    /// supplied. ECS accepts so many optional fields that round-tripping
    /// the raw JSON is simpler and more faithful than modeling a struct
    /// with hundreds of members per container.
    #[serde(default)]
    pub container_definitions: Vec<Value>,
    pub status: String,
    pub task_role_arn: Option<String>,
    pub execution_role_arn: Option<String>,
    pub network_mode: Option<String>,
    #[serde(default)]
    pub requires_compatibilities: Vec<String>,
    #[serde(default)]
    pub compatibilities: Vec<String>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub pid_mode: Option<String>,
    pub ipc_mode: Option<String>,
    #[serde(default)]
    pub volumes: Vec<Value>,
    #[serde(default)]
    pub placement_constraints: Vec<Value>,
    pub proxy_configuration: Option<Value>,
    #[serde(default)]
    pub inference_accelerators: Vec<Value>,
    pub ephemeral_storage: Option<Value>,
    pub runtime_platform: Option<Value>,
    #[serde(default)]
    pub requires_attributes: Vec<Value>,
    pub registered_at: DateTime<Utc>,
    pub registered_by: Option<String>,
    pub deregistered_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    pub enable_fault_injection: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub task_arn: String,
    pub task_id: String,
    pub cluster_arn: String,
    pub cluster_name: String,
    pub task_definition_arn: String,
    pub family: String,
    pub revision: i32,
    /// Container instance this task was placed on. Populated for EC2 /
    /// EXTERNAL launch types after placement evaluation.
    #[serde(default)]
    pub container_instance_arn: Option<String>,
    /// Capacity provider this task was placed on. Set when the launch
    /// went through a `capacityProviderStrategy`; absent for direct
    /// `launchType=EC2/FARGATE` calls. AWS's Task model emits this at
    /// the top level next to `launchType`.
    #[serde(default)]
    pub capacity_provider_name: Option<String>,
    /// Current lifecycle state: PROVISIONING, PENDING, RUNNING,
    /// DEPROVISIONING, STOPPED.
    pub last_status: String,
    /// What the caller asked for: usually RUNNING, or STOPPED once
    /// `StopTask` / `StopService` hits.
    pub desired_status: String,
    pub launch_type: String,
    pub platform_version: Option<String>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    #[serde(default)]
    pub containers: Vec<Container>,
    #[serde(default)]
    pub overrides: Value,
    pub started_by: Option<String>,
    pub group: Option<String>,
    pub connectivity: String,
    pub stop_code: Option<String>,
    pub stopped_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub stopping_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub pull_started_at: Option<DateTime<Utc>>,
    pub pull_stopped_at: Option<DateTime<Utc>>,
    pub connectivity_at: Option<DateTime<Utc>>,
    pub started_by_ref_id: Option<String>,
    pub execution_role_arn: Option<String>,
    pub task_role_arn: Option<String>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    /// Log destination derived from the first container's awslogs driver.
    /// `None` when no awslogs driver is configured — captured stdout/stderr
    /// is still stored on the task for introspection.
    pub awslogs: Option<AwsLogsConfig>,
    /// Captured stdout/stderr from the container. Populated after the
    /// container exits. Kept here so the introspection endpoint can serve
    /// logs even when no awslogs driver is configured.
    #[serde(default)]
    pub captured_logs: String,
    /// Task protection state (UpdateTaskProtection). When set, scale-in
    /// and update-service deployments skip this task until the expiry.
    pub protection: Option<TaskProtection>,
    /// Whether ECS Exec is enabled on this task. Inherited from the
    /// owning service's `enableExecuteCommand` flag (or supplied
    /// directly on RunTask). Gated when `ExecuteCommand` is invoked.
    #[serde(default)]
    pub enable_execute_command: bool,
    /// Network attachments (ENI, elastic-inference, etc.) populated when
    /// the task uses `awsvpc` network mode. Synthetic for fakecloud.
    #[serde(default)]
    pub attachments: Vec<TaskAttachment>,
    /// Per-task volume configurations (EBS / FSx) supplied at RunTask or
    /// inherited from the service. Stored as raw JSON.
    #[serde(default)]
    pub volume_configurations: Vec<Value>,
    /// Task set this task belongs to. Populated when the task is spawned
    /// by a service using the CODE_DEPLOY deployment controller.
    #[serde(default)]
    pub task_set_arn: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskAttachment {
    pub id: String,
    #[serde(rename = "type")]
    pub attachment_type: String,
    pub status: String,
    #[serde(default)]
    pub details: Vec<AttachmentDetail>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttachmentDetail {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskProtection {
    pub enabled: bool,
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Container {
    pub container_arn: String,
    pub name: String,
    pub image: String,
    pub task_arn: String,
    pub last_status: String,
    pub exit_code: Option<i64>,
    pub reason: Option<String>,
    pub runtime_id: Option<String>,
    pub essential: bool,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub memory_reservation: Option<String>,
    #[serde(default)]
    pub network_bindings: Vec<Value>,
    #[serde(default)]
    pub network_interfaces: Vec<Value>,
    pub health_status: Option<String>,
    pub managed_agents: Option<Value>,
    /// Resolved image digest captured at pull time. AWS surfaces this on
    /// DescribeTasks so callers can pin which exact image revision a task
    /// is running. `None` until the runtime resolves it post-pull.
    #[serde(default)]
    pub image_digest: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AwsLogsConfig {
    pub group: String,
    pub stream_prefix: Option<String>,
    pub region: String,
    pub container_name: String,
}

impl AwsLogsConfig {
    pub fn stream_name(&self, task_id: &str) -> String {
        match &self.stream_prefix {
            Some(p) => format!("{}/{}/{}", p, self.container_name, task_id),
            None => format!("{}/{}", self.container_name, task_id),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub at: DateTime<Utc>,
    pub event_type: String,
    pub task_arn: Option<String>,
    pub cluster_arn: Option<String>,
    pub last_status: Option<String>,
    pub detail: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    pub service_name: String,
    pub service_arn: String,
    pub cluster_name: String,
    pub cluster_arn: String,
    pub task_definition_arn: String,
    pub family: String,
    pub revision: i32,
    pub desired_count: i32,
    pub running_count: i32,
    pub pending_count: i32,
    pub launch_type: String,
    pub status: String,
    pub scheduling_strategy: String,
    pub deployment_controller: String,
    pub minimum_healthy_percent: Option<i32>,
    pub maximum_percent: Option<i32>,
    /// Deployment circuit breaker config (opt-in via deploymentConfiguration).
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    #[serde(default)]
    pub deployments: Vec<Deployment>,
    #[serde(default)]
    pub load_balancers: Vec<Value>,
    #[serde(default)]
    pub service_registries: Vec<Value>,
    #[serde(default)]
    pub placement_constraints: Vec<Value>,
    #[serde(default)]
    pub placement_strategy: Vec<Value>,
    #[serde(default)]
    pub network_configuration: Option<Value>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub role_arn: Option<String>,
    /// Fargate platform version label ("LATEST", "1.4.0", etc). Echoed
    /// back on DescribeServices.
    #[serde(default)]
    pub platform_version: Option<String>,
    /// Seconds an ECS service waits before failing a task on health
    /// check failures while a load balancer is still warming up.
    #[serde(default)]
    pub health_check_grace_period_seconds: Option<i32>,
    /// Whether ECS Exec is enabled for tasks launched by this service.
    #[serde(default)]
    pub enable_execute_command: bool,
    /// When true, ECS automatically tags tasks/ENIs with cluster + service
    /// metadata. Off by default to match AWS.
    #[serde(default)]
    pub enable_ecs_managed_tags: bool,
    /// Tag-propagation source: "TASK_DEFINITION", "SERVICE", or "NONE".
    /// We model the AWS shape — None here means "NONE" was the effective
    /// value when the service was created.
    #[serde(default)]
    pub propagate_tags: Option<String>,
    /// Mixed capacity-provider weights that the service uses instead of
    /// (or alongside) `launch_type`. Stored as raw JSON since the field
    /// is a list of `{ capacityProvider, weight, base }` records.
    #[serde(default)]
    pub capacity_provider_strategy: Vec<Value>,
    /// AZ-rebalancing toggle for ALB-attached services. ENABLED |
    /// DISABLED (default). Surface field for AvailabilityZoneRebalancing.
    #[serde(default)]
    pub availability_zone_rebalancing: Option<String>,
    /// Per-service volume configurations (EBS / FSx) inherited by tasks
    /// launched under this service. Stored as raw JSON.
    #[serde(default)]
    pub volume_configurations: Vec<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub enable: bool,
    pub rollback: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Deployment {
    pub deployment_id: String,
    pub status: String,
    pub task_definition_arn: String,
    pub desired_count: i32,
    pub pending_count: i32,
    pub running_count: i32,
    pub failed_tasks: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub launch_type: String,
    pub rollout_state: String,
    pub rollout_state_reason: Option<String>,
    /// Lifecycle hooks attached via `deploymentConfiguration.lifecycleHooks`,
    /// preserved as the raw JSON the caller supplied so every optional field
    /// round-trips faithfully.
    #[serde(default)]
    pub lifecycle_hooks: Vec<Value>,
    /// ID of the PAUSE lifecycle hook the deployment is currently waiting on.
    /// `Some` while the deployment is paused; cleared by
    /// `ContinueServiceDeployment`.
    #[serde(default)]
    pub pending_hook_id: Option<String>,
    /// Lifecycle stage the deployment paused at (e.g.
    /// `POST_PRODUCTION_TRAFFIC_SHIFT`). `None` when the deployment is not
    /// paused on a hook.
    #[serde(default)]
    pub lifecycle_stage: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContainerInstance {
    pub container_instance_arn: String,
    pub ec2_instance_id: Option<String>,
    pub cluster_name: String,
    pub cluster_arn: String,
    pub status: String,
    pub version: i64,
    pub version_info: Option<Value>,
    pub agent_connected: bool,
    pub agent_update_status: Option<String>,
    pub remaining_resources: Vec<Value>,
    pub registered_resources: Vec<Value>,
    pub running_tasks_count: i32,
    pub pending_tasks_count: i32,
    pub registered_at: DateTime<Utc>,
    #[serde(default)]
    pub attributes: Vec<AttributeRef>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    pub capacity_provider_name: Option<String>,
    pub health_status: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttributeRef {
    pub name: String,
    pub value: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attribute {
    pub cluster_name: String,
    pub target_type: String,
    pub target_id: String,
    pub name: String,
    pub value: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapacityProvider {
    pub name: String,
    pub arn: String,
    pub status: String,
    pub auto_scaling_group_provider: Option<Value>,
    pub update_status: Option<String>,
    pub update_status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSet {
    pub task_set_id: String,
    pub task_set_arn: String,
    pub service_arn: String,
    pub cluster_arn: String,
    pub service_name: String,
    pub cluster_name: String,
    pub external_id: Option<String>,
    pub status: String,
    pub task_definition: String,
    pub computed_desired_count: i32,
    pub pending_count: i32,
    pub running_count: i32,
    pub launch_type: Option<String>,
    pub platform_version: Option<String>,
    pub scale: Option<Value>,
    pub stability_status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub load_balancers: Vec<Value>,
    #[serde(default)]
    pub service_registries: Vec<Value>,
    #[serde(default)]
    pub capacity_provider_strategy: Vec<Value>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
}

/// Daemon task definition. Same structural shape as a regular
/// TaskDefinition but registered via `RegisterDaemonTaskDefinition` and
/// kept in a separate per-family revision counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonTaskDefinition {
    pub family: String,
    pub revision: i32,
    pub task_definition_arn: String,
    pub status: String,
    pub container_definitions: Vec<Value>,
    pub task_role_arn: Option<String>,
    pub execution_role_arn: Option<String>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    #[serde(default)]
    pub volumes: Vec<Value>,
    pub registered_at: DateTime<Utc>,
    pub deregistered_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
}

/// Daemon resource. Daemons run one task per matching capacity
/// provider in the cluster. Modeled after the ECS Service struct
/// since the lifecycle / status / deployment story is parallel.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Daemon {
    pub daemon_name: String,
    pub daemon_arn: String,
    pub cluster_arn: String,
    pub cluster_name: String,
    pub daemon_task_definition_arn: String,
    pub status: String,
    pub deployment_arn: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub capacity_provider_arns: Vec<String>,
    pub deployment_configuration: Option<Value>,
    pub propagate_tags: Option<String>,
    pub enable_ecs_managed_tags: bool,
    pub enable_execute_command: bool,
    pub client_token: Option<String>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    /// Revision history of deployment ARNs in chronological order.
    #[serde(default)]
    pub deployment_history: Vec<String>,
    /// Active task IDs spawned by this daemon.
    #[serde(default)]
    pub task_arns: Vec<String>,
}

/// Single deployment record. Created on every CreateDaemon /
/// UpdateDaemon and retained so DescribeDaemonDeployments and
/// DescribeDaemonRevisions have something to return.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonDeployment {
    pub deployment_arn: String,
    pub daemon_arn: String,
    pub daemon_name: String,
    pub cluster_arn: String,
    pub task_definition_arn: String,
    pub status: String,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 2026 Express Gateway service — serverless container service with
/// integrated load balancing, health checks, and autoscaling.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpressGatewayService {
    pub service_name: String,
    pub service_arn: String,
    pub cluster_arn: String,
    pub cluster_name: String,
    pub status: String,
    pub execution_role_arn: String,
    pub infrastructure_role_arn: String,
    pub task_role_arn: Option<String>,
    pub primary_container: Value,
    pub network_configuration: Option<Value>,
    pub health_check_path: Option<String>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub scaling_target: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
}

impl EcsState {
    /// Composite key for daemon storage (`cluster_name/daemon_name`).
    pub fn daemon_key(cluster: &str, name: &str) -> String {
        format!("{}/{}", cluster, name)
    }

    /// Composite key for express-gateway storage (`cluster_name/service_name`).
    pub fn express_gateway_key(cluster: &str, name: &str) -> String {
        format!("{}/{}", cluster, name)
    }

    /// Allocate the next monotonic revision for a daemon task family.
    pub fn allocate_daemon_revision(&mut self, family: &str) -> i32 {
        let entry = self
            .next_daemon_revision
            .entry(family.to_string())
            .or_insert(0);
        *entry += 1;
        *entry
    }

    /// Build a daemon ARN for a (cluster, name) pair under this account/region.
    pub fn daemon_arn(&self, cluster: &str, name: &str) -> String {
        fakecloud_aws::arn::Arn::new(
            "ecs",
            &self.region,
            &self.account_id,
            &format!("daemon/{}/{}", cluster, name),
        )
        .to_string()
    }

    /// Build an express-gateway service ARN.
    pub fn express_gateway_arn(&self, cluster: &str, name: &str) -> String {
        fakecloud_aws::arn::Arn::new(
            "ecs",
            &self.region,
            &self.account_id,
            &format!("express-gateway-service/{}/{}", cluster, name),
        )
        .to_string()
    }

    /// Build a daemon task definition ARN for a `family:revision` pair.
    pub fn daemon_task_definition_arn(&self, family: &str, revision: i32) -> String {
        fakecloud_aws::arn::Arn::new(
            "ecs",
            &self.region,
            &self.account_id,
            &format!("daemon-task-definition/{}:{}", family, revision),
        )
        .to_string()
    }

    /// Build a daemon deployment ARN.
    pub fn daemon_deployment_arn(&self, daemon_name: &str, deployment_id: &str) -> String {
        fakecloud_aws::arn::Arn::new(
            "ecs",
            &self.region,
            &self.account_id,
            &format!("daemon-deployment/{}/{}", daemon_name, deployment_id),
        )
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_cluster_name_defaults_to_default() {
        assert_eq!(EcsState::resolve_cluster_name(None), "default");
        assert_eq!(EcsState::resolve_cluster_name(Some("")), "default");
        assert_eq!(EcsState::resolve_cluster_name(Some("   ")), "default");
    }

    #[test]
    fn resolve_cluster_name_strips_arn_prefix() {
        assert_eq!(
            EcsState::resolve_cluster_name(Some("arn:aws:ecs:us-east-1:111122223333:cluster/prod")),
            "prod"
        );
    }

    #[test]
    fn resolve_cluster_name_passes_through_name() {
        assert_eq!(EcsState::resolve_cluster_name(Some("prod")), "prod");
    }

    #[test]
    fn allocate_revision_monotonic() {
        let mut s = EcsState::new("111122223333", "us-east-1");
        assert_eq!(s.allocate_revision("web"), 1);
        assert_eq!(s.allocate_revision("web"), 2);
        assert_eq!(s.allocate_revision("worker"), 1);
        assert_eq!(s.allocate_revision("web"), 3);
    }

    #[test]
    fn cluster_arn_format() {
        let s = EcsState::new("111122223333", "us-east-1");
        assert_eq!(
            s.cluster_arn("prod"),
            "arn:aws:ecs:us-east-1:111122223333:cluster/prod"
        );
    }

    #[test]
    fn task_definition_arn_format() {
        let s = EcsState::new("111122223333", "us-east-1");
        assert_eq!(
            s.task_definition_arn("web", 3),
            "arn:aws:ecs:us-east-1:111122223333:task-definition/web:3"
        );
    }

    #[test]
    fn task_arn_long_format_default() {
        let s = EcsState::new("111122223333", "us-east-1");
        assert_eq!(
            s.task_arn("prod", "abc123"),
            "arn:aws:ecs:us-east-1:111122223333:task/prod/abc123"
        );
    }

    #[test]
    fn task_arn_short_when_disabled() {
        let mut s = EcsState::new("111122223333", "us-east-1");
        s.account_setting_defaults
            .insert("taskLongArnFormat".into(), "disabled".into());
        assert_eq!(
            s.task_arn("prod", "abc123"),
            "arn:aws:ecs:us-east-1:111122223333:task/abc123"
        );
    }

    #[test]
    fn service_arn_short_when_disabled() {
        let mut s = EcsState::new("111122223333", "us-east-1");
        s.account_setting_defaults
            .insert("serviceLongArnFormat".into(), "disabled".into());
        assert_eq!(
            s.service_arn("prod", "web"),
            "arn:aws:ecs:us-east-1:111122223333:service/web"
        );
    }

    #[test]
    fn container_instance_arn_short_when_disabled() {
        let mut s = EcsState::new("111122223333", "us-east-1");
        s.account_setting_defaults
            .insert("containerInstanceLongArnFormat".into(), "disabled".into());
        assert_eq!(
            s.container_instance_arn("prod", "i-abc"),
            "arn:aws:ecs:us-east-1:111122223333:container-instance/i-abc"
        );
    }

    #[test]
    fn principal_setting_overrides_default() {
        let mut s = EcsState::new("111122223333", "us-east-1");
        s.account_setting_defaults
            .insert("taskLongArnFormat".into(), "disabled".into());
        let principal = "arn:aws:iam::111122223333:user/alice".to_string();
        let mut p = BTreeMap::new();
        p.insert("taskLongArnFormat".into(), "enabled".into());
        s.principal_account_settings.insert(principal.clone(), p);
        assert_eq!(
            s.effective_account_setting("taskLongArnFormat", Some(&principal))
                .as_deref(),
            Some("enabled")
        );
        // Without principal, default wins.
        assert_eq!(
            s.effective_account_setting("taskLongArnFormat", None)
                .as_deref(),
            Some("disabled")
        );
    }

    #[test]
    fn reset_clears_all() {
        let mut s = EcsState::new("111122223333", "us-east-1");
        s.clusters.insert(
            "prod".to_string(),
            Cluster::new("prod", s.cluster_arn("prod")),
        );
        s.allocate_revision("web");
        s.account_setting_defaults
            .insert("serviceLongArnFormat".into(), "enabled".into());
        s.reset();
        assert!(s.clusters.is_empty());
        assert!(s.next_revision.is_empty());
        assert!(s.account_setting_defaults.is_empty());
    }
}
