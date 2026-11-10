//! The service side of durable executions: starting executions from
//! `Invoke`, invoking the function with the durable payload, re-invoking it
//! when waits elapse / retries come due / callbacks and chained invokes
//! complete, and enforcing the timers.
//!
//! A single scheduler task drives everything. It wakes every
//! [`TICK`] (or at once when kicked), advances each running execution's
//! timers under the Lambda state lock, then spawns the invocations and
//! chained calls that became due. At most one invocation runs per
//! execution at a time; the function itself talks back to the service
//! through `CheckpointDurableExecution` / `GetDurableExecutionState`, which
//! reach this crate over the regular Lambda endpoint (the container's
//! `AWS_ENDPOINT_URL`).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use crate::runtime::ContainerRuntime;
use crate::state::{LambdaFunction, LambdaState, SharedLambdaState};

type Accounts = fakecloud_core::multi_account::MultiRegionState<LambdaState>;

use super::engine::{self, ChainedOutcome, InvocationOutcome, NewExecution};
use super::model::{
    self, DurableExecution, OpStatus, OpType, ParentLink, DEFAULT_RETENTION_DAYS, FAILED, STOPPED,
    SUCCEEDED, TIMED_OUT,
};

/// How often the scheduler advances timers when nothing kicks it.
const TICK: StdDuration = StdDuration::from_millis(200);

/// Default `ExecutionTimeout` for a function whose stored `DurableConfig`
/// lacks one (state written before the value was required).
const DEFAULT_EXECUTION_TIMEOUT: i64 = 86_400;

/// Shared handle to the durable execution service.
#[derive(Clone)]
pub struct DurableHub {
    inner: Arc<HubInner>,
}

struct HubInner {
    state: SharedLambdaState,
    runtime: RwLock<Option<Arc<ContainerRuntime>>>,
    persist: RwLock<Option<fakecloud_persistence::SnapshotHook>>,
    persist_pending: AtomicBool,
    kick: tokio::sync::Notify,
    /// Executions with an invocation task alive in this process.
    running: Mutex<HashSet<String>>,
    /// Chained invokes (execution ARN, operation id) with a task alive in
    /// this process. A dispatched one missing here was lost with a previous
    /// process and is dispatched again.
    chained_live: Mutex<HashSet<(String, String)>>,
    /// A snapshot write is in flight.
    persisting: AtomicBool,
    started: AtomicBool,
}

/// The configuration a durable function's executions run with.
pub(crate) struct DurableSettings {
    pub execution_timeout: i64,
    pub retention_days: i64,
    pub kms_key_arn: Option<String>,
}

impl DurableSettings {
    pub(crate) fn of(func: &LambdaFunction) -> Option<DurableSettings> {
        let cfg = func.durable_config.as_ref().filter(|c| c.is_object())?;
        Some(DurableSettings {
            execution_timeout: cfg
                .get("ExecutionTimeout")
                .and_then(Value::as_i64)
                .unwrap_or(DEFAULT_EXECUTION_TIMEOUT),
            retention_days: cfg
                .get("RetentionPeriodInDays")
                .and_then(Value::as_i64)
                .unwrap_or(DEFAULT_RETENTION_DAYS),
            kms_key_arn: cfg
                .get("KMSKeyArn")
                .and_then(Value::as_str)
                .filter(|k| !k.is_empty())
                .map(str::to_string),
        })
    }
}

/// The function record an execution pinned to `version` runs.
pub(crate) fn pinned_function(
    st: &LambdaState,
    function_name: &str,
    version: &str,
) -> Option<LambdaFunction> {
    if version != "$LATEST" {
        if let Some(f) = st
            .function_version_snapshots
            .get(function_name)
            .and_then(|m| m.get(version))
        {
            return Some(f.clone());
        }
    }
    st.functions.get(function_name).cloned()
}

/// What starting an execution produced.
pub(crate) enum StartOutcome {
    Started(String),
    /// An execution of this name already exists with the same input.
    Existing(String),
    /// An execution of this name already exists with another input.
    Conflict(String),
}

/// Start an execution of `function_name` pinned to `version` (state lock
/// held by the caller). Applies the execution-name idempotency rules.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_execution(
    st: &mut LambdaState,
    region: &str,
    function_name: &str,
    version: &str,
    settings: &DurableSettings,
    name: Option<&str>,
    input: &str,
    tenant_id: Option<&str>,
    parent: Option<ParentLink>,
) -> StartOutcome {
    if let Some(n) = name {
        if let Some(existing) = st
            .durable_executions
            .values()
            .find(|e| e.function_name == function_name && e.name == n)
        {
            return if existing.input_payload == input {
                StartOutcome::Existing(existing.arn.clone())
            } else {
                StartOutcome::Conflict(existing.arn.clone())
            };
        }
    }
    let now = model::now();
    let id = model::new_execution_id();
    let name = name
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let unqualified = crate::state::function_arn(region, &st.account_id, function_name);
    let arn = model::execution_arn(&unqualified, version, &name, &id);
    let exec = engine::create_execution(
        NewExecution {
            arn: arn.clone(),
            id,
            name,
            function_name: function_name.to_string(),
            function_arn: format!("{unqualified}:{version}"),
            version: version.to_string(),
            input: input.to_string(),
            execution_timeout: settings.execution_timeout,
            retention_days: settings.retention_days,
            kms_key_arn: settings.kms_key_arn.clone(),
            trace_id: model::new_trace_header(now),
            tenant_id: tenant_id.map(str::to_string),
            parent,
        },
        now,
    );
    st.durable_executions.insert(arn.clone(), exec);
    StartOutcome::Started(arn)
}

/// A chained invoke the scheduler is about to run.
struct ChainedJob {
    account: String,
    region: String,
    parent_arn: String,
    op_id: String,
    func: LambdaFunction,
    layers: Vec<Vec<u8>>,
    payload: String,
}

impl DurableHub {
    pub fn new(state: SharedLambdaState) -> Self {
        Self {
            inner: Arc::new(HubInner {
                state,
                runtime: RwLock::new(None),
                persist: RwLock::new(None),
                persist_pending: AtomicBool::new(false),
                kick: tokio::sync::Notify::new(),
                running: Mutex::new(HashSet::new()),
                chained_live: Mutex::new(HashSet::new()),
                persisting: AtomicBool::new(false),
                started: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn state(&self) -> &SharedLambdaState {
        &self.inner.state
    }

    pub(crate) fn has_runtime(&self) -> bool {
        self.inner.runtime.read().is_some()
    }

    pub fn set_runtime(&self, runtime: Arc<ContainerRuntime>) {
        *self.inner.runtime.write() = Some(runtime);
    }

    pub fn set_persist(&self, hook: Option<fakecloud_persistence::SnapshotHook>) {
        *self.inner.persist.write() = hook;
    }

    /// Persist the Lambda state on the scheduler's next pass.
    pub(crate) fn request_persist(&self) {
        self.inner.persist_pending.store(true, Ordering::Release);
    }

    /// Run the scheduler now instead of at the next tick.
    pub(crate) fn kick(&self) {
        self.ensure_started();
        self.inner.kick.notify_one();
    }

    /// Spawn the scheduler task once. Executions restored from a snapshot
    /// resume as soon as it runs. A no-op outside a Tokio runtime.
    pub fn ensure_started(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let hub = self.clone();
        tokio::spawn(async move { hub.run().await });
    }

    async fn run(self) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(TICK) => {}
                _ = self.inner.kick.notified() => {}
            }
            self.pass();
            self.persist_if_requested();
        }
    }

    /// Write the snapshot in the background when one was requested and no
    /// write is in flight; requests made meanwhile coalesce into the next.
    fn persist_if_requested(&self) {
        if self.inner.persisting.load(Ordering::Acquire)
            || !self.inner.persist_pending.swap(false, Ordering::AcqRel)
        {
            return;
        }
        let Some(hook) = self.inner.persist.read().clone() else {
            return;
        };
        self.inner.persisting.store(true, Ordering::Release);
        let hub = self.clone();
        tokio::spawn(async move {
            hook().await;
            hub.inner.persisting.store(false, Ordering::Release);
        });
    }

    /// One scheduler pass.
    fn pass(&self) {
        let now = model::now();
        let mut launch: Vec<(String, String, String)> = Vec::new();
        let mut jobs: Vec<ChainedJob> = Vec::new();
        let mut reports: Vec<(String, String, String, String, ChainedOutcome)> = Vec::new();
        let mut changed = false;
        {
            let mut accounts = self.inner.state.write();
            let running = self.inner.running.lock().clone();
            let chained_live = self.inner.chained_live.lock().clone();
            let has_runtime = self.inner.runtime.read().is_some();
            // Chained invokes are collected first; dispatching them needs
            // the whole state.
            let mut pending_dispatch: Vec<(String, String, String, String, String, bool)> =
                Vec::new();
            for (account, region, st) in accounts.iter_regional_mut() {
                let mut expired = Vec::new();
                let functions: HashSet<String> = st.functions.keys().cloned().collect();
                for (arn, exec) in st.durable_executions.iter_mut() {
                    if engine::tick(exec, now) {
                        changed = true;
                    }
                    if exec.is_running() && !functions.contains(&exec.function_name) {
                        // The function was deleted: nothing can run the
                        // execution any more.
                        let e = json!({
                            "ErrorType": "ResourceNotFoundException",
                            "ErrorMessage": format!("Function not found: {}", exec.function_arn),
                        });
                        engine::finish(exec, FAILED, None, Some(e), now);
                        changed = true;
                    }
                    if exec.is_running() {
                        if exec.invocation.is_some() && !running.contains(arn) {
                            // The invocation died with the process that ran
                            // it (restart): run the function again.
                            exec.invocation = None;
                            exec.wake = true;
                        }
                        let retry_due = exec.retry.as_ref().is_some_and(|r| r.at <= now);
                        if exec.invocation.is_none()
                            && (exec.wake || retry_due)
                            && !running.contains(arn)
                        {
                            if has_runtime {
                                launch.push((account.to_string(), region.to_string(), arn.clone()));
                            } else {
                                // Without a container runtime the function
                                // can never run; fail now instead of retrying
                                // until the execution times out.
                                engine::finish(exec, FAILED, None, Some(no_runtime_error()), now);
                                changed = true;
                                continue;
                            }
                        }
                        for op in &exec.operations {
                            let Some(c) = op.chained.as_ref() else {
                                continue;
                            };
                            if op.op_type != OpType::ChainedInvoke || op.status != OpStatus::Started
                            {
                                continue;
                            }
                            let lost = c.dispatched
                                && c.child_execution_arn.is_none()
                                && !chained_live.contains(&(arn.clone(), op.id.clone()));
                            if !c.dispatched || lost {
                                pending_dispatch.push((
                                    account.to_string(),
                                    region.to_string(),
                                    arn.clone(),
                                    op.id.clone(),
                                    c.function_name.clone(),
                                    lost,
                                ));
                            }
                        }
                    } else {
                        if let Some(parent) = exec.parent.take() {
                            reports.push((
                                account.to_string(),
                                region.to_string(),
                                parent.execution_arn,
                                parent.operation_id,
                                child_outcome(exec),
                            ));
                            changed = true;
                        }
                        let retention = chrono::Duration::days(exec.retention_days.max(1));
                        if exec.end_timestamp.is_some_and(|e| e + retention < now) {
                            expired.push(arn.clone());
                        }
                    }
                }
                for arn in expired {
                    st.durable_executions.remove(&arn);
                    changed = true;
                }
            }
            for (account, region, arn, op_id, function_ref, redispatch) in pending_dispatch {
                if let Some(job) = dispatch_chained(
                    &mut accounts,
                    &account,
                    &region,
                    &arn,
                    &op_id,
                    &function_ref,
                    redispatch,
                ) {
                    jobs.push(job);
                }
                changed = true;
            }
            for (account, region, parent_arn, op_id, outcome) in reports {
                if let Some(parent) = accounts
                    .regional_get_mut(&account, &region)
                    .and_then(|st| st.durable_executions.get_mut(&parent_arn))
                {
                    engine::complete_chained(parent, &op_id, outcome, now);
                }
            }
        }
        if changed {
            self.request_persist();
        }
        for (account, region, arn) in launch {
            if self.inner.running.lock().insert(arn.clone()) {
                let hub = self.clone();
                tokio::spawn(async move { hub.run_invocations(account, region, arn).await });
            }
        }
        for job in jobs {
            self.inner
                .chained_live
                .lock()
                .insert((job.parent_arn.clone(), job.op_id.clone()));
            let hub = self.clone();
            tokio::spawn(async move { hub.run_chained(job).await });
        }
    }

    /// Invoke the function for `arn` until it has nothing more to do right
    /// now (it finished, or it is waiting on a timer or an external event).
    async fn run_invocations(self, account: String, region: String, arn: String) {
        loop {
            let request_id = uuid::Uuid::new_v4().to_string();
            let prepared = {
                let mut accounts = self.inner.state.write();
                let func = accounts.regional(&account, &region).and_then(|st| {
                    let e = st.durable_executions.get(&arn)?;
                    Some(pinned_function(st, &e.function_name, &e.version))
                });
                let layers = func
                    .as_ref()
                    .and_then(|f| f.as_ref())
                    .map(|f| crate::state::attached_layer_zips(&accounts, f))
                    .unwrap_or_default();
                match accounts
                    .regional_get_mut(&account, &region)
                    .and_then(|st| st.durable_executions.get_mut(&arn))
                {
                    Some(exec) if exec.is_running() && exec.invocation.is_none() => {
                        match func.flatten() {
                            Some(func) => {
                                let payload =
                                    engine::begin_invocation(exec, &request_id, model::now());
                                Some((func, layers, payload))
                            }
                            None => {
                                let e = json!({
                                    "ErrorType": "ResourceNotFoundException",
                                    "ErrorMessage": format!("Function not found: {}", exec.function_arn),
                                });
                                engine::finish(exec, FAILED, None, Some(e), model::now());
                                None
                            }
                        }
                    }
                    _ => None,
                }
            };
            let Some((func, layers, payload)) = prepared else {
                break;
            };
            self.request_persist();
            let outcome = self.invoke_function(&func, &layers, &payload).await;
            let again = {
                let mut accounts = self.inner.state.write();
                match accounts
                    .regional_get_mut(&account, &region)
                    .and_then(|st| st.durable_executions.get_mut(&arn))
                {
                    Some(exec) => {
                        engine::complete_invocation(exec, &request_id, outcome, model::now())
                    }
                    None => false,
                }
            };
            self.request_persist();
            if !again {
                break;
            }
        }
        self.inner.running.lock().remove(&arn);
        // Something may have become due while the invocation ran.
        self.inner.kick.notify_one();
    }

    async fn invoke_function(
        &self,
        func: &LambdaFunction,
        layers: &[Vec<u8>],
        payload: &Value,
    ) -> InvocationOutcome {
        let runtime = self.inner.runtime.read().clone();
        let Some(runtime) = runtime else {
            return InvocationOutcome::FunctionError(no_runtime_error());
        };
        let bytes = serde_json::to_vec(payload).unwrap_or_default();
        match runtime.invoke(func, &bytes, layers).await {
            Ok(out) => match function_error(&out) {
                Some(e) => InvocationOutcome::FunctionError(e),
                None => {
                    InvocationOutcome::Output(serde_json::from_slice(&out).unwrap_or(Value::Null))
                }
            },
            Err(e) => InvocationOutcome::FunctionError(json!({
                "ErrorType": "Runtime.InvocationFailed",
                "ErrorMessage": e.to_string(),
            })),
        }
    }

    /// Run a non-durable chained invoke and record its outcome.
    async fn run_chained(self, job: ChainedJob) {
        let runtime = self.inner.runtime.read().clone();
        let outcome = match runtime {
            None => ChainedOutcome {
                status: OpStatus::Failed,
                result: None,
                error: Some(json!({
                    "ErrorType": "ServiceException",
                    "ErrorMessage": "Docker/Podman is required for Lambda execution but is not available.",
                })),
            },
            Some(rt) => match rt
                .invoke(&job.func, job.payload.as_bytes(), &job.layers)
                .await
            {
                Ok(out) => match function_error(&out) {
                    Some(e) => ChainedOutcome {
                        status: OpStatus::Failed,
                        result: None,
                        error: Some(e),
                    },
                    None => ChainedOutcome {
                        status: OpStatus::Succeeded,
                        result: Some(String::from_utf8_lossy(&out).into_owned()),
                        error: None,
                    },
                },
                Err(e) => ChainedOutcome {
                    status: OpStatus::Failed,
                    result: None,
                    error: Some(json!({
                        "ErrorType": "Runtime.InvocationFailed",
                        "ErrorMessage": e.to_string(),
                    })),
                },
            },
        };
        {
            let mut accounts = self.inner.state.write();
            if let Some(exec) = accounts
                .regional_get_mut(&job.account, &job.region)
                .and_then(|st| st.durable_executions.get_mut(&job.parent_arn))
            {
                engine::complete_chained(exec, &job.op_id, outcome, model::now());
            }
        }
        self.inner
            .chained_live
            .lock()
            .remove(&(job.parent_arn.clone(), job.op_id.clone()));
        self.request_persist();
        self.inner.kick.notify_one();
    }

    /// Wait until `arn` is no longer running, up to `max_wait`.
    pub(crate) async fn wait_for_end(
        &self,
        account: &str,
        region: &str,
        arn: &str,
        max_wait: StdDuration,
    ) -> Option<DurableExecution> {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            let snapshot = {
                let accounts = self.inner.state.read();
                accounts
                    .regional(account, region)
                    .and_then(|st| st.durable_executions.get(arn))
                    .cloned()
            };
            match snapshot {
                Some(e) if !e.is_running() => return Some(e),
                Some(_) if tokio::time::Instant::now() < deadline => {}
                _ => return None,
            }
            tokio::time::sleep(StdDuration::from_millis(100)).await;
        }
    }
}

/// The error an execution fails with when no container runtime can run it.
pub(crate) fn no_runtime_error() -> Value {
    json!({
        "ErrorType": "ServiceException",
        "ErrorMessage": format!(
            "Docker/Podman is required for Lambda execution but is not available. {}",
            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
        ),
    })
}

/// Whether `version` of `function_name` exists (`$LATEST` always does).
pub(crate) fn version_exists(st: &LambdaState, function_name: &str, version: &str) -> bool {
    version == "$LATEST"
        || st
            .function_versions
            .get(function_name)
            .is_some_and(|vs| vs.iter().any(|v| v == version))
        || st
            .function_version_snapshots
            .get(function_name)
            .is_some_and(|m| m.contains_key(version))
}

/// The `ErrorObject` a function error envelope (`errorMessage` /
/// `errorType` / `stackTrace`) stands for, or `None` for a normal return.
fn function_error(bytes: &[u8]) -> Option<Value> {
    let v: Value = serde_json::from_slice(bytes).ok()?;
    let obj = v.as_object()?;
    if obj.contains_key("Status") {
        return None;
    }
    if !obj.contains_key("errorMessage") && !obj.contains_key("errorType") {
        return None;
    }
    let mut e = serde_json::Map::new();
    if let Some(t) = obj.get("errorType").and_then(Value::as_str) {
        e.insert("ErrorType".into(), json!(t));
    }
    if let Some(m) = obj.get("errorMessage").and_then(Value::as_str) {
        e.insert("ErrorMessage".into(), json!(m));
    }
    if let Some(st) = obj.get("stackTrace").and_then(Value::as_array) {
        let lines: Vec<Value> = st.iter().filter(|l| l.is_string()).cloned().collect();
        if !lines.is_empty() {
            e.insert("StackTrace".into(), Value::Array(lines));
        }
    }
    Some(Value::Object(e))
}

/// How a finished child execution reports to its chained-invoke parent.
fn child_outcome(child: &DurableExecution) -> ChainedOutcome {
    let status = match child.status.as_str() {
        SUCCEEDED => OpStatus::Succeeded,
        TIMED_OUT => OpStatus::TimedOut,
        STOPPED => OpStatus::Stopped,
        _ => OpStatus::Failed,
    };
    ChainedOutcome {
        status,
        result: child
            .result
            .clone()
            .filter(|_| status == OpStatus::Succeeded),
        error: child
            .error
            .clone()
            .filter(|_| status != OpStatus::Succeeded),
    }
}

/// Resolve and dispatch one chained invoke (state lock held). Durable
/// targets get a child execution reporting back to the operation; plain
/// targets are returned as a job to run.
fn dispatch_chained(
    accounts: &mut Accounts,
    account: &str,
    region: &str,
    parent_arn: &str,
    op_id: &str,
    function_ref: &str,
    redispatch: bool,
) -> Option<ChainedJob> {
    let now = model::now();
    let (target_account, target_region, name) =
        crate::state::function_location(function_ref, account, region);
    let (target_account, target_region, name) = (
        target_account.to_string(),
        target_region.to_string(),
        name.to_string(),
    );
    let qualifier = crate::service::qualifier_from_function_ref(function_ref);
    let fail = |accounts: &mut Accounts, error: Value| {
        if let Some(parent) = accounts
            .regional_get_mut(account, region)
            .and_then(|st| st.durable_executions.get_mut(parent_arn))
        {
            if !redispatch {
                engine::chained_dispatched(parent, op_id, None, None);
            }
            engine::complete_chained(
                parent,
                op_id,
                ChainedOutcome {
                    status: OpStatus::Failed,
                    result: None,
                    error: Some(error),
                },
                now,
            );
        }
    };
    let not_found = || {
        json!({
            "ErrorType": "ResourceNotFoundException",
            "ErrorMessage": format!(
                "Function not found: {}",
                crate::state::function_arn(&target_region, &target_account, &name)
            ),
        })
    };
    // Chained invocations stay within the account (and the region).
    if target_account != account || target_region != region {
        fail(accounts, not_found());
        return None;
    }
    let (func, version, payload) = {
        let Some(st) = accounts.regional(account, region) else {
            fail(accounts, not_found());
            return None;
        };
        let Some(func) = st.functions.get(&name).cloned() else {
            fail(accounts, not_found());
            return None;
        };
        if let Some(q) = qualifier.as_deref() {
            let alias_known = q == "$LATEST"
                || q.chars().all(|c| c.is_ascii_digit())
                || st.aliases.contains_key(&format!("{name}:{q}"));
            if !alias_known {
                fail(accounts, not_found());
                return None;
            }
        }
        let version = crate::service::resolve_qualifier_to_version(st, &name, qualifier.as_deref())
            .unwrap_or_else(|| "$LATEST".to_string());
        if !version_exists(st, &name, &version) {
            fail(accounts, not_found());
            return None;
        }
        let pinned = pinned_function(st, &name, &version).unwrap_or(func);
        let payload = st
            .durable_executions
            .get(parent_arn)
            .and_then(|p| p.op(op_id))
            .and_then(|o| o.chained.as_ref())
            .and_then(|c| c.payload.clone())
            .unwrap_or_else(|| "{}".to_string());
        (pinned, version, payload)
    };

    if let Some(settings) = DurableSettings::of(&func) {
        if qualifier.is_none() {
            fail(
                accounts,
                json!({
                    "ErrorType": "InvalidParameterValueException",
                    "ErrorMessage": "You cannot invoke a durable function using an unqualified ARN.",
                }),
            );
            return None;
        }
        let st = accounts.regional_mut(account, region);
        let tenant = st
            .durable_executions
            .get(parent_arn)
            .and_then(|p| p.op(op_id))
            .and_then(|o| o.chained.as_ref())
            .and_then(|c| c.tenant_id.clone());
        let child = match start_execution(
            st,
            region,
            &name,
            &version,
            &settings,
            None,
            &payload,
            tenant.as_deref(),
            Some(ParentLink {
                execution_arn: parent_arn.to_string(),
                operation_id: op_id.to_string(),
            }),
        ) {
            StartOutcome::Started(a) | StartOutcome::Existing(a) | StartOutcome::Conflict(a) => a,
        };
        if let Some(parent) = st.durable_executions.get_mut(parent_arn) {
            if redispatch {
                engine::set_chained_child(parent, op_id, &child);
            } else {
                engine::chained_dispatched(parent, op_id, Some(&version), Some(&child));
            }
        }
        return None;
    }

    let layers = crate::state::attached_layer_zips(accounts, &func);
    if let Some(parent) = accounts
        .regional_get_mut(account, region)
        .and_then(|st| st.durable_executions.get_mut(parent_arn))
    {
        if !redispatch {
            engine::chained_dispatched(parent, op_id, Some(&version), None);
        }
    }
    if func.code_zip.is_none() && func.package_type != "Image" {
        if let Some(parent) = accounts
            .regional_get_mut(account, region)
            .and_then(|st| st.durable_executions.get_mut(parent_arn))
        {
            engine::complete_chained(
                parent,
                op_id,
                ChainedOutcome {
                    status: OpStatus::Failed,
                    result: None,
                    error: Some(json!({
                        "ErrorType": "InvalidParameterValueException",
                        "ErrorMessage": "Function has no deployment package",
                    })),
                },
                now,
            );
        }
        return None;
    }
    Some(ChainedJob {
        account: account.to_string(),
        region: region.to_string(),
        parent_arn: parent_arn.to_string(),
        op_id: op_id.to_string(),
        func,
        layers,
        payload,
    })
}

/// The body and function-error flag a synchronous `Invoke` answers with
/// once the execution has ended.
pub(crate) fn sync_response_body(exec: &DurableExecution) -> (Vec<u8>, bool) {
    match exec.status.as_str() {
        SUCCEEDED => (exec.result.clone().unwrap_or_default().into_bytes(), false),
        FAILED | STOPPED | TIMED_OUT => {
            let mut m = serde_json::Map::new();
            if let Some(e) = &exec.error {
                if let Some(msg) = e.get("ErrorMessage") {
                    m.insert("errorMessage".into(), msg.clone());
                }
                if let Some(t) = e.get("ErrorType") {
                    m.insert("errorType".into(), t.clone());
                }
            }
            (Value::Object(m).to_string().into_bytes(), true)
        }
        _ => (Vec::new(), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;

    const ACCOUNT: &str = "123456789012";

    fn hub_with(functions: &[(&str, bool)]) -> DurableHub {
        let state: SharedLambdaState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                ACCOUNT,
                "us-east-1",
                "http://localhost:4566",
            ),
        ));
        {
            let mut accounts = state.write();
            let st = accounts.regional_mut(ACCOUNT, "us-east-1");
            for (name, durable) in functions {
                st.functions.insert(
                    name.to_string(),
                    LambdaFunction {
                        function_name: name.to_string(),
                        function_arn: crate::state::function_arn("us-east-1", ACCOUNT, name),
                        durable_config: durable.then(|| json!({"ExecutionTimeout": 600})),
                        ..Default::default()
                    },
                );
            }
        }
        DurableHub::new(state)
    }

    fn start(hub: &DurableHub) -> String {
        let mut accounts = hub.state().write();
        let st = accounts.regional_mut(ACCOUNT, "us-east-1");
        let settings = DurableSettings::of(&st.functions["f"]).unwrap();
        match start_execution(
            st,
            "us-east-1",
            "f",
            "$LATEST",
            &settings,
            None,
            "{}",
            None,
            None,
        ) {
            StartOutcome::Started(a) => a,
            _ => unreachable!(),
        }
    }

    fn with_exec<T>(hub: &DurableHub, arn: &str, f: impl FnOnce(&mut DurableExecution) -> T) -> T {
        let mut accounts = hub.state().write();
        f(accounts
            .regional_mut(ACCOUNT, "us-east-1")
            .durable_executions
            .get_mut(arn)
            .unwrap())
    }

    #[tokio::test]
    async fn executions_without_a_runtime_or_function_fail_instead_of_retrying() {
        let hub = hub_with(&[("f", true)]);
        let arn = start(&hub);
        hub.pass();
        with_exec(&hub, &arn, |e| {
            assert_eq!(e.status, FAILED);
            assert_eq!(e.error.as_ref().unwrap()["ErrorType"], "ServiceException");
        });

        let hub = hub_with(&[("f", true)]);
        let arn = start(&hub);
        hub.state()
            .write()
            .regional_mut(ACCOUNT, "us-east-1")
            .functions
            .remove("f");
        hub.pass();
        with_exec(&hub, &arn, |e| {
            assert_eq!(e.status, FAILED);
            assert_eq!(
                e.error.as_ref().unwrap()["ErrorType"],
                "ResourceNotFoundException"
            );
        });
    }

    #[tokio::test]
    async fn a_chained_invoke_lost_with_its_process_is_dispatched_again() {
        let hub = hub_with(&[("f", true), ("target", false)]);
        let arn = start(&hub);
        with_exec(&hub, &arn, |e| {
            engine::begin_invocation(e, "r1", model::now());
            let token = e.invocation.as_ref().unwrap().token.clone().unwrap();
            engine::checkpoint(
                e,
                &token,
                None,
                &[json!({"Id":"ci","Type":"CHAINED_INVOKE","Action":"START","ChainedInvokeOptions":{"FunctionName":"target"}})],
                model::now(),
            )
            .unwrap();
            engine::complete_invocation(
                e,
                "r1",
                InvocationOutcome::Output(json!({"Status":"PENDING"})),
                model::now(),
            );
            // Dispatched by a process that is gone: no live job tracks it.
            engine::chained_dispatched(e, "ci", Some("$LATEST"), None);
        });
        hub.pass();
        with_exec(&hub, &arn, |e| {
            // Dispatched again (the target has no code, so it fails), without
            // a second ChainedInvokeStarted.
            assert_eq!(e.op("ci").unwrap().status, OpStatus::Failed);
            let started = e
                .events
                .iter()
                .filter(|ev| ev["EventType"] == "ChainedInvokeStarted")
                .count();
            assert_eq!(started, 1);
        });
    }

    #[test]
    fn function_error_envelopes_become_error_objects() {
        assert_eq!(
            function_error(br#"{"errorMessage":"crashed invocation","errorType":"Error","stackTrace":["at x"]}"#),
            Some(json!({"ErrorType":"Error","ErrorMessage":"crashed invocation","StackTrace":["at x"]}))
        );
        assert_eq!(function_error(br#"{"Status":"PENDING"}"#), None);
        assert_eq!(function_error(br#""ok""#), None);
    }

    #[test]
    fn sync_bodies_follow_the_recorded_shapes() {
        let mut e = engine::create_execution(
            NewExecution {
                arn: "a".into(),
                id: "i".into(),
                name: "n".into(),
                function_name: "f".into(),
                function_arn: "f".into(),
                version: "$LATEST".into(),
                input: "{}".into(),
                execution_timeout: 15,
                retention_days: 1,
                kms_key_arn: None,
                trace_id: String::new(),
                tenant_id: None,
                parent: None,
            },
            model::now(),
        );
        engine::finish(
            &mut e,
            TIMED_OUT,
            None,
            Some(json!({"ErrorMessage":"Execution timed out after 15 seconds."})),
            model::now(),
        );
        let (body, err) = sync_response_body(&e);
        assert!(err);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"errorMessage":"Execution timed out after 15 seconds."})
        );
    }
}
