use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_aws::arn::Arn;

pub type SharedStepFunctionsState =
    Arc<RwLock<fakecloud_core::multi_account::MultiRegionState<StepFunctionsState>>>;

impl fakecloud_core::multi_account::AccountState for StepFunctionsState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// v3: state partitioned by (account, region). v2 kept one state per
/// account; v1 a single account's.
pub const STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

pub type StepFunctionsSnapshot =
    fakecloud_core::multi_account::RegionalSnapshot<StepFunctionsState>;

/// Parse a persisted Step Functions snapshot, migrating pre-regional schemas
/// by moving every resource into the region its ARN names.
pub fn parse_stepfunctions_snapshot(
    bytes: &[u8],
) -> Result<StepFunctionsSnapshot, serde_json::Error> {
    fakecloud_core::multi_account::parse_regional_snapshot(
        bytes,
        STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION,
        |s: StepFunctionsState| {
            let (account, region) = (s.account_id.clone(), s.region.clone());
            fakecloud_core::multi_account::RegionalState::from_legacy(&account, &region, "", s)
        },
    )
}

impl fakecloud_core::multi_account::SplitByRegion for StepFunctionsState {
    /// State machines, executions, activities, versions, aliases and map
    /// runs are all keyed by their own ARN and go to its region; an activity
    /// task token follows its activity (a `.waitForTaskToken` token has none
    /// and lands in the default region).
    fn split_by_region(self, into: &mut fakecloud_core::multi_account::RegionalState<Self>) {
        use fakecloud_aws::arn::region_of;
        for (k, v) in self.state_machines {
            into.region_or_default_mut(region_of(&k))
                .state_machines
                .insert(k, v);
        }
        for (k, v) in self.executions {
            into.region_or_default_mut(region_of(&k))
                .executions
                .insert(k, v);
        }
        for (k, v) in self.activities {
            into.region_or_default_mut(region_of(&k))
                .activities
                .insert(k, v);
        }
        for (k, v) in self.state_machine_versions {
            into.region_or_default_mut(region_of(&k))
                .state_machine_versions
                .insert(k, v);
        }
        for (k, v) in self.state_machine_aliases {
            into.region_or_default_mut(region_of(&k))
                .state_machine_aliases
                .insert(k, v);
        }
        for (k, v) in self.map_runs {
            into.region_or_default_mut(region_of(&k))
                .map_runs
                .insert(k, v);
        }
        for (k, v) in self.task_tokens {
            let region = region_of(&v.activity_arn).map(str::to_string);
            into.region_or_default_mut(region.as_deref())
                .task_tokens
                .insert(k, v);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepFunctionsState {
    pub account_id: String,
    pub region: String,
    /// State machines keyed by ARN.
    #[serde(default)]
    pub state_machines: BTreeMap<String, StateMachine>,
    /// Executions keyed by execution ARN.
    #[serde(default)]
    pub executions: BTreeMap<String, Execution>,
    #[serde(default)]
    pub activities: BTreeMap<String, Activity>,
    #[serde(default)]
    pub state_machine_versions: BTreeMap<String, StateMachineVersion>,
    #[serde(default)]
    pub state_machine_aliases: BTreeMap<String, StateMachineAlias>,
    #[serde(default)]
    pub map_runs: BTreeMap<String, MapRun>,
    /// Pending task tokens issued for sync activities + their outcome.
    #[serde(default)]
    pub task_tokens: BTreeMap<String, TaskTokenState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub name: String,
    pub arn: String,
    pub creation_date: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineVersion {
    pub state_machine_arn: String,
    pub version: i64,
    pub revision_id: String,
    pub description: String,
    pub creation_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineAlias {
    pub name: String,
    pub arn: String,
    pub description: String,
    pub routing_configuration: Vec<AliasRoute>,
    pub creation_date: DateTime<Utc>,
    pub update_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasRoute {
    pub state_machine_version_arn: String,
    pub weight: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapRun {
    pub map_run_arn: String,
    pub execution_arn: String,
    pub max_concurrency: i32,
    pub tolerated_failure_percentage: f64,
    pub tolerated_failure_count: i64,
    pub status: String,
    pub start_date: DateTime<Utc>,
    pub stop_date: Option<DateTime<Utc>>,
    /// Total number of items the distributed Map iterated over.
    #[serde(default)]
    pub total_count: i64,
    /// Iterations that completed successfully.
    #[serde(default)]
    pub succeeded_count: i64,
    /// Iterations that failed.
    #[serde(default)]
    pub failed_count: i64,
    /// The state machine its child executions run:
    /// `stateMachine:<machine>/<label>` (see [`MapRunChildren`]).
    #[serde(default)]
    pub state_machine_arn: String,
    /// The Map state's `Label`, or the stable id AWS names it by without one.
    #[serde(default)]
    pub label: String,
    /// The definition its child executions run: the `ItemProcessor` without
    /// its `ProcessorConfig`, as `DescribeStateMachineForExecution` reports it.
    #[serde(default)]
    pub child_definition: String,
    /// `ProcessorConfig.ExecutionType` of its child executions.
    #[serde(default = "default_child_type")]
    pub child_type: StateMachineType,
}

fn default_child_type() -> StateMachineType {
    StateMachineType::Standard
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTokenState {
    pub activity_arn: String,
    /// PENDING (waiting for `GetActivityTask` to dequeue) /
    /// IN_PROGRESS (worker has picked it up) /
    /// SUCCEEDED / FAILED / TIMED_OUT.
    pub status: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub cause: Option<String>,
    /// Input the state machine wanted the worker to process. `None`
    /// for tokens minted by external `GetActivityTask` callers without
    /// any associated activity execution (legacy synthetic path).
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default = "default_now")]
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// Per AWS docs: state machine fails the task if no heartbeat in
    /// this many seconds while the worker is running.
    #[serde(default)]
    pub heartbeat_seconds: Option<i64>,
    /// Overall timeout for the task; counted from `created_at`.
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
}

fn default_now() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachine {
    pub name: String,
    pub arn: String,
    pub definition: String,
    pub role_arn: String,
    pub machine_type: StateMachineType,
    pub status: StateMachineStatus,
    pub creation_date: DateTime<Utc>,
    pub update_date: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub revision_id: String,
    pub logging_configuration: Option<Value>,
    pub tracing_configuration: Option<Value>,
    pub description: String,
    #[serde(default)]
    pub encryption_configuration: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateMachineType {
    Standard,
    Express,
}

impl StateMachineType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::Express => "EXPRESS",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "STANDARD" => Some(Self::Standard),
            "EXPRESS" => Some(Self::Express),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateMachineStatus {
    Active,
    Deleting,
}

impl StateMachineStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Deleting => "DELETING",
        }
    }
}

tokio::task_local! {
    /// The execution whose Task is calling StartExecution, set only around that
    /// call. A new execution records it as its `parent_execution_arn`.
    pub(crate) static STARTED_BY_EXECUTION: String;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub execution_arn: String,
    pub state_machine_arn: String,
    pub state_machine_name: String,
    pub name: String,
    pub status: ExecutionStatus,
    pub input: Option<String>,
    pub output: Option<String>,
    pub start_date: DateTime<Utc>,
    pub stop_date: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub cause: Option<String>,
    pub history_events: Vec<HistoryEvent>,
    /// Parent execution ARN when this execution was started by another
    /// state machine via `arn:aws:states:::states:startExecution[.sync]`.
    /// `None` for top-level executions started by external callers.
    #[serde(default)]
    pub parent_execution_arn: Option<String>,
    /// True when this execution was created by `StartSyncExecution`
    /// (EXPRESS state machines only). Distinguishes it from regular
    /// async executions in introspection endpoints.
    #[serde(default)]
    pub is_sync: bool,
    /// Billed duration in milliseconds, populated on terminal state for
    /// sync executions. Mirrors `billingDetails.billedDurationInMilliseconds`
    /// from the StartSyncExecution response.
    #[serde(default)]
    pub billed_duration_ms: Option<i64>,
    /// Billed memory in MB for sync executions. Mirrors
    /// `billingDetails.billedMemoryUsedInMB`.
    #[serde(default)]
    pub billed_memory_mb: Option<i64>,
    /// The state machine's role when the execution started, recorded on its
    /// `ExecutionStarted` history event.
    #[serde(default)]
    pub role_arn: String,
    /// How many times `RedriveExecution` has redriven this execution
    /// (`redriveCount`, `$$.Execution.RedriveCount`). Defaults to 0 for state
    /// persisted before it was tracked.
    #[serde(default)]
    pub redrive_count: u32,
    /// The map run that started this execution, for a child execution of a
    /// distributed Map.
    #[serde(default)]
    pub map_run_arn: Option<String>,
    /// How many items a map run child execution processes (`itemCount`): one,
    /// or its batch's size under an `ItemBatcher`.
    #[serde(default)]
    pub item_count: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Aborted,
    PendingRedrive,
}

impl ExecutionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::TimedOut => "TIMED_OUT",
            Self::Aborted => "ABORTED",
            Self::PendingRedrive => "PENDING_REDRIVE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub id: i64,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub previous_event_id: i64,
    pub details: Value,
}

impl StepFunctionsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            state_machines: BTreeMap::new(),
            executions: BTreeMap::new(),
            activities: BTreeMap::new(),
            state_machine_versions: BTreeMap::new(),
            state_machine_aliases: BTreeMap::new(),
            map_runs: BTreeMap::new(),
            task_tokens: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.state_machines.clear();
        self.executions.clear();
        self.activities.clear();
        self.state_machine_versions.clear();
        self.state_machine_aliases.clear();
        self.map_runs.clear();
        self.task_tokens.clear();
    }

    pub fn state_machine_arn(&self, region: &str, name: &str) -> String {
        state_machine_arn(region, &self.account_id, name)
    }

    pub fn execution_arn(
        &self,
        region: &str,
        state_machine_name: &str,
        execution_name: &str,
    ) -> String {
        execution_arn(region, &self.account_id, state_machine_name, execution_name)
    }

    /// The ARN for a new execution of a `machine_type` state machine: the
    /// name-keyed `execution:` ARN for STANDARD, a fresh `express:` ARN for
    /// EXPRESS (see [`express_execution_arn`]).
    pub fn new_execution_arn(
        &self,
        region: &str,
        machine_type: StateMachineType,
        state_machine_name: &str,
        execution_name: &str,
    ) -> String {
        match machine_type {
            StateMachineType::Standard => {
                self.execution_arn(region, state_machine_name, execution_name)
            }
            StateMachineType::Express => {
                express_execution_arn(region, &self.account_id, state_machine_name, execution_name)
            }
        }
    }
}

/// A Step Functions ARN (`arn:<partition>:states:<region>:<account>:<resource>`)
/// in `region`'s partition.
pub fn states_arn(region: &str, account_id: &str, resource: &str) -> String {
    Arn::regional("states", region, account_id, resource).to_string()
}

/// The ARN of a state machine.
pub fn state_machine_arn(region: &str, account_id: &str, name: &str) -> String {
    states_arn(region, account_id, &format!("stateMachine:{name}"))
}

/// The ARN of a standard-workflow execution.
pub fn execution_arn(
    region: &str,
    account_id: &str,
    state_machine_name: &str,
    execution_name: &str,
) -> String {
    states_arn(
        region,
        account_id,
        &format!("execution:{state_machine_name}:{execution_name}"),
    )
}

/// Whether `arn` is an EXPRESS execution ARN (`...:express:<machine>:...`).
pub fn is_express_execution_arn(arn: &str) -> bool {
    arn.split(':').nth(5) == Some("express")
}

/// Whether a state machine or execution ARN names a labeled state machine:
/// `<machine>/<label>`, the state machine of a map run's child executions.
/// A state machine's own name can't contain a `/`.
pub fn is_labeled_arn(arn: &str) -> bool {
    arn.split(':').nth(6).is_some_and(|name| name.contains('/'))
}

/// The ARNs a distributed Map's run and its child executions are named by,
/// as recorded: everything hangs off `<machine>/<label>`.
pub struct MapRunChildren {
    /// `mapRun:<machine>/<label>:<id>`.
    pub map_run_arn: String,
    /// `stateMachine:<machine>/<label>`.
    pub state_machine_arn: String,
    pub label: String,
}

impl MapRunChildren {
    /// The map run a Map state named `state_name` starts in `execution_arn`
    /// (STANDARD or EXPRESS). Without a `Label` AWS names the run by an id
    /// stable for the state machine and state, as recorded.
    pub fn new(execution_arn: &str, state_name: &str, label: Option<&str>) -> Option<Self> {
        let (prefix, tail) = execution_arn
            .split_once(":execution:")
            .or_else(|| execution_arn.split_once(":express:"))?;
        let machine = tail.split(':').next()?;
        let label = match label {
            Some(label) => label.to_string(),
            None => uuid::Uuid::new_v3(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{prefix}:stateMachine:{machine}#{state_name}").as_bytes(),
            )
            .to_string(),
        };
        Some(Self {
            map_run_arn: format!("{prefix}:mapRun:{machine}/{label}:{}", uuid::Uuid::new_v4()),
            state_machine_arn: format!("{prefix}:stateMachine:{machine}/{label}"),
            label,
        })
    }
}

/// The ARN and name of a map run's `index`th child execution, as recorded: a
/// name-derived id under `<machine>/<label>`, which an EXPRESS child follows
/// with its own id, as any EXPRESS execution does.
pub fn map_run_child_execution(
    map_run_arn: &str,
    index: usize,
    child_type: StateMachineType,
) -> Option<(String, String)> {
    let (prefix, run) = map_run_arn.split_once(":mapRun:")?;
    let (path, _) = run.rsplit_once(':')?;
    let id = uuid::Uuid::new_v3(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{map_run_arn}#{index}").as_bytes(),
    );
    Some(match child_type {
        StateMachineType::Standard => {
            let name = id.to_string();
            (format!("{prefix}:execution:{path}:{name}"), name)
        }
        StateMachineType::Express => {
            let name = format!("{id}:{}", uuid::Uuid::new_v4());
            (format!("{prefix}:express:{path}:{name}"), name)
        }
    })
}

/// A fresh ARN for one EXPRESS execution: `express:<machine>:<name>:<id>`.
///
/// EXPRESS names can be reused immediately and StartExecution is never
/// idempotent for them, so every start is its own execution. The trailing id
/// keeps two starts under the same name apart.
pub fn express_execution_arn(
    region: &str,
    account_id: &str,
    state_machine_name: &str,
    execution_name: &str,
) -> String {
    states_arn(
        region,
        account_id,
        &format!(
            "express:{state_machine_name}:{execution_name}:{}",
            uuid::Uuid::new_v4()
        ),
    )
}

/// The ARN of an activity.
pub fn activity_arn(region: &str, account_id: &str, name: &str) -> String {
    states_arn(region, account_id, &format!("activity:{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_machine_type_as_str() {
        assert_eq!(StateMachineType::Standard.as_str(), "STANDARD");
        assert_eq!(StateMachineType::Express.as_str(), "EXPRESS");
    }

    #[test]
    fn state_machine_type_parse() {
        assert_eq!(
            StateMachineType::parse("STANDARD"),
            Some(StateMachineType::Standard)
        );
        assert_eq!(
            StateMachineType::parse("EXPRESS"),
            Some(StateMachineType::Express)
        );
        assert_eq!(StateMachineType::parse("bogus"), None);
    }

    #[test]
    fn state_machine_status_as_str() {
        assert_eq!(StateMachineStatus::Active.as_str(), "ACTIVE");
        assert_eq!(StateMachineStatus::Deleting.as_str(), "DELETING");
    }

    #[test]
    fn execution_status_as_str() {
        assert_eq!(ExecutionStatus::Running.as_str(), "RUNNING");
        assert_eq!(ExecutionStatus::Succeeded.as_str(), "SUCCEEDED");
        assert_eq!(ExecutionStatus::Failed.as_str(), "FAILED");
        assert_eq!(ExecutionStatus::TimedOut.as_str(), "TIMED_OUT");
        assert_eq!(ExecutionStatus::Aborted.as_str(), "ABORTED");
        assert_eq!(ExecutionStatus::PendingRedrive.as_str(), "PENDING_REDRIVE");
    }

    #[test]
    fn state_machine_arn_format() {
        let state = StepFunctionsState::new("123456789012", "us-east-1");
        assert_eq!(
            state.state_machine_arn("us-east-1", "my-sm"),
            "arn:aws:states:us-east-1:123456789012:stateMachine:my-sm"
        );
    }

    #[test]
    fn execution_arn_format() {
        let state = StepFunctionsState::new("123456789012", "us-east-1");
        assert_eq!(
            state.execution_arn("us-east-1", "sm", "exec-1"),
            "arn:aws:states:us-east-1:123456789012:execution:sm:exec-1"
        );
    }

    #[test]
    fn arns_use_request_region_not_server_default() {
        // Server default region is us-east-1, but a client whose credential
        // scope resolves to eu-central-1 must get eu-central-1 ARNs.
        let state = StepFunctionsState::new("123456789012", "us-east-1");
        assert_eq!(
            state.state_machine_arn("eu-central-1", "my-sm"),
            "arn:aws:states:eu-central-1:123456789012:stateMachine:my-sm"
        );
        assert_eq!(
            state.execution_arn("eu-central-1", "sm", "exec-1"),
            "arn:aws:states:eu-central-1:123456789012:execution:sm:exec-1"
        );
    }

    #[test]
    fn state_reset_clears_all() {
        let mut state = StepFunctionsState::new("123456789012", "us-east-1");
        state.state_machines.insert(
            "x".to_string(),
            StateMachine {
                name: "sm".to_string(),
                arn: "arn:aws:states:us-east-1:123:stateMachine:sm".to_string(),
                definition: "{}".to_string(),
                role_arn: "r".to_string(),
                machine_type: StateMachineType::Standard,
                status: StateMachineStatus::Active,
                creation_date: Utc::now(),
                update_date: Utc::now(),
                tags: BTreeMap::new(),
                revision_id: "v1".to_string(),
                logging_configuration: None,
                tracing_configuration: None,
                description: String::new(),
                encryption_configuration: None,
            },
        );
        state.reset();
        assert!(state.state_machines.is_empty());
        assert!(state.executions.is_empty());
    }
}
