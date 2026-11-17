//! Stopping ECS tasks: the stop lifecycle, `dependsOn`-ordered container
//! stops, and the network teardown that must run even once the task record
//! is gone.
//!
//! As on ECS (task lifecycle: RUNNING -> DEACTIVATING -> STOPPING ->
//! DEPROVISIONING -> STOPPED), a stopping task first leaves its load
//! balancer target groups (DEACTIVATING), then its containers are stopped
//! (STOPPING), then its ENI is released (DEPROVISIONING) and it is STOPPED.
//! The target group's deregistration delay is not waited out.
//!
//! Containers stop in reverse `dependsOn` order: a container is stopped once
//! every container depending on it has stopped. Containers with no
//! dependency between them stop in parallel, so a task's stop takes the
//! longest chain of `stopTimeout`s, not their sum.

use std::time::Duration;

use chrono::Utc;

use super::config::ContainerCli;
use super::*;

/// One running task container, as the stop paths need it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskContainerHandle {
    pub name: String,
    pub container_id: String,
    /// The container's `stopTimeout` (SIGTERM-to-SIGKILL grace).
    pub stop_timeout: u32,
    /// The containers this one depends on (`dependsOn[].containerName`).
    pub depends_on: Vec<String>,
}

impl TaskContainerHandle {
    pub(crate) fn new(plan: &ContainerPlan, container_id: String) -> Self {
        Self {
            name: plan.container_name.clone(),
            container_id,
            stop_timeout: stop_timeout_secs(plan),
            depends_on: plan
                .depends_on
                .iter()
                .map(|d| d.container_name.clone())
                .collect(),
        }
    }
}

/// Network teardown of a stopping task, captured from its record when it
/// starts stopping.
#[derive(Clone, Debug, Default)]
pub(crate) struct TaskTeardown {
    region: String,
    eni_ip: Option<String>,
    eni_id: Option<String>,
}

/// Group `containers` into stop waves (indices into `containers`): every
/// container in a wave is depended on only by containers in earlier waves.
/// Dependencies naming a container outside `containers` are ignored; a cycle
/// (rejected at register time) puts what is left in one final wave.
pub(crate) fn stop_waves(containers: &[TaskContainerHandle]) -> Vec<Vec<usize>> {
    let mut remaining: Vec<usize> = (0..containers.len()).collect();
    let mut waves = Vec::new();
    while !remaining.is_empty() {
        let (wave, rest): (Vec<usize>, Vec<usize>) = remaining.iter().partition(|&&i| {
            !remaining
                .iter()
                .any(|&j| j != i && containers[j].depends_on.contains(&containers[i].name))
        });
        if wave.is_empty() {
            waves.push(rest);
            break;
        }
        waves.push(wave);
        remaining = rest;
    }
    waves
}

/// Stop `containers` with `docker stop --time <stopTimeout>`, wave by wave,
/// each wave in parallel. Returns once every stop call has returned.
pub(crate) async fn stop_in_waves(cli: ContainerCli, containers: Vec<TaskContainerHandle>) {
    for wave in stop_waves(&containers) {
        let mut stops = tokio::task::JoinSet::new();
        for i in wave {
            let c = containers[i].clone();
            let cli = cli.clone();
            stops.spawn(async move {
                let _ = cli
                    .output(
                        [
                            "stop",
                            "--time",
                            &c.stop_timeout.to_string(),
                            &c.container_id,
                        ],
                        stop_call_timeout(c.stop_timeout),
                    )
                    .await;
            });
        }
        stops.join_all().await;
    }
}

/// Record a stop request on the task the way `StopTask` does: desired
/// status `STOPPED`, `stoppingAt`, and a `UserInitiated` stop code with
/// `reason`, unless a stop was already requested.
pub fn request_task_stop(task: &mut crate::state::Task, reason: &str) {
    if task.desired_status == "STOPPED" && task.stop_code.is_some() {
        return;
    }
    task.desired_status = "STOPPED".into();
    task.stopping_at = task.stopping_at.or(Some(Utc::now()));
    task.stopped_reason = Some(reason.to_string());
    task.stop_code = Some("UserInitiated".into());
}

fn set_last_status(state: &SharedEcsState, account_id: &str, task_id: &str, status: &str) {
    let mut accounts = state.write();
    if let Some(task) = accounts
        .get_mut(account_id)
        .and_then(|s| s.tasks.get_mut(task_id))
    {
        if task.last_status != "STOPPED" {
            task.last_status = status.to_string();
        }
    }
}

impl EcsRuntime {
    /// Start stopping a task: its desired status becomes `STOPPED`, it
    /// leaves its load balancer target groups (DEACTIVATING) and moves to
    /// STOPPING, and its network teardown is captured so it completes even
    /// if the task record is dropped (reset, stack delete) before the
    /// containers are gone. A task already STOPPED keeps its status.
    pub fn begin_stopping(&self, state: &SharedEcsState, account_id: &str, task_id: &str) {
        let (targets, teardown) = {
            let mut accounts = state.write();
            let Some(s) = accounts.get_mut(account_id) else {
                return;
            };
            let region = s.region.clone();
            let Some(task) = s.tasks.get_mut(task_id) else {
                return;
            };
            task.desired_status = "STOPPED".into();
            task.stopping_at = task.stopping_at.or(Some(Utc::now()));
            // A STOPPED record already released what it held; its targets
            // and ENI IP may belong to a live replacement by now.
            if task.last_status == "STOPPED" {
                return;
            }
            let task = task.clone();
            let teardown = TaskTeardown {
                region: fakecloud_aws::arn::region_of(&task.task_arn)
                    .map(str::to_string)
                    .unwrap_or(region),
                eni_ip: super::eni::task_eni_ip(&task),
                eni_id: super::eni::task_eni_id(&task),
            };
            (compute_elbv2_targets(s, &task), teardown)
        };
        {
            let mut teardowns = self.teardowns.lock();
            if teardowns.contains_key(task_id) || self.released.lock().contains(task_id) {
                // Already stopping (its targets were deregistered then), or
                // already released.
                return;
            }
            teardowns.insert(task_id.to_string(), teardown);
        }
        if !targets.is_empty() {
            set_last_status(state, account_id, task_id, "DEACTIVATING");
        }
        if let Some(bus) = &self.delivery_bus {
            for (tg_arn, tg_targets) in targets {
                bus.deregister_elbv2_targets(account_id, &tg_arn, tg_targets);
            }
        }
        set_last_status(state, account_id, task_id, "STOPPING");
    }

    /// Whether `task_id` started stopping (its targets were deregistered or
    /// its network released): it must not register targets again.
    pub(crate) fn stopping_began(&self, task_id: &str) -> bool {
        self.teardowns.lock().contains_key(task_id) || self.released.lock().contains(task_id)
    }

    /// The task execution's own release of its network once its containers
    /// are gone (see [`Self::release_network`]). Once per task: after it, a
    /// late release could remove the route or ENI of a replacement that
    /// reused the address, so nothing releases this task again.
    pub(crate) fn finish_teardown(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        region: &str,
        task_id: &str,
    ) {
        if !self.released.lock().insert(task_id.to_string()) {
            self.teardowns.lock().remove(task_id);
            return;
        }
        self.release_network(state, account_id, region, task_id);
    }

    /// Release for a task no live execution will finalize (it never started,
    /// or it died): what it holds now is released, without closing the
    /// execution's own release, which stays authoritative should the
    /// execution still come to run. Every step is idempotent by id.
    pub(crate) fn release_orphaned(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        region: &str,
        task_id: &str,
    ) {
        if self.released.lock().contains(task_id) {
            self.teardowns.lock().remove(task_id);
            return;
        }
        self.release_network(state, account_id, region, task_id);
    }

    /// Release a stopping task's network: its load balancer targets (if
    /// [`Self::begin_stopping`] didn't already), the data-plane route of its
    /// awsvpc ports, and its ENI. Works from the teardown captured when the
    /// task started stopping, so it completes even when the task record is
    /// gone.
    fn release_network(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        region: &str,
        task_id: &str,
    ) {
        let captured = self.teardowns.lock().remove(task_id);
        let teardown = match captured {
            Some(mut t) => {
                // The record, while it exists, is current: an ENI attached
                // after the stop began (the task was still launching) is
                // released too.
                let accounts = state.read();
                if let Some(task) = accounts.get(account_id).and_then(|s| s.tasks.get(task_id)) {
                    t.eni_ip = super::eni::task_eni_ip(task).or(t.eni_ip);
                    t.eni_id = super::eni::task_eni_id(task).or(t.eni_id);
                }
                t
            }
            None => {
                // Never went through `begin_stopping`: deregister now.
                let accounts = state.read();
                let Some(s) = accounts.get(account_id) else {
                    return;
                };
                let Some(task) = s.tasks.get(task_id) else {
                    return;
                };
                let targets = compute_elbv2_targets(s, task);
                let teardown = TaskTeardown {
                    region: region.to_string(),
                    eni_ip: super::eni::task_eni_ip(task),
                    eni_id: super::eni::task_eni_id(task),
                };
                drop(accounts);
                if let Some(bus) = &self.delivery_bus {
                    for (tg_arn, tg_targets) in targets {
                        bus.deregister_elbv2_targets(account_id, &tg_arn, tg_targets);
                    }
                }
                teardown
            }
        };
        // The task's published awsvpc ports are gone with its containers.
        if let Some(ip) = &teardown.eni_ip {
            fakecloud_core::dataplane::unregister_target(account_id, ip);
        }
        // Its ENI goes with the task, as on ECS.
        if let (Some(bus), Some(eni_id)) = (&self.delivery_bus, &teardown.eni_id) {
            bus.delete_task_eni(account_id, &teardown.region, eni_id);
        }
    }

    /// The DEPROVISIONING step of a task whose containers are gone: release
    /// its network ([`Self::finish_teardown`]), showing DEPROVISIONING while
    /// an ENI is detached. The caller then finalizes it STOPPED.
    pub(crate) fn deprovision(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        region: &str,
        task_id: &str,
    ) {
        let has_eni = self
            .teardowns
            .lock()
            .get(task_id)
            .is_some_and(|t| t.eni_id.is_some());
        if has_eni {
            set_last_status(state, account_id, task_id, "DEPROVISIONING");
        }
        self.finish_teardown(state, account_id, region, task_id);
    }

    /// Register the "execution finished" signal of a task about to run.
    pub(super) fn track_task_execution(&self, task_id: &str) -> tokio::sync::watch::Sender<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        self.task_done.lock().insert(task_id.to_string(), rx);
        tx
    }

    /// Mark a task's execution finished (finalized STOPPED or failed).
    pub(super) fn task_execution_finished(
        &self,
        task_id: &str,
        done: &tokio::sync::watch::Sender<bool>,
    ) {
        // Waiters hold their own receiver; task IDs are never reused.
        let _ = done.send(true);
        self.task_done.lock().remove(task_id);
    }

    /// Stop every container behind a task, in `dependsOn` order, with each
    /// container's `stopTimeout`. As on ECS, this returns while the
    /// containers are still stopping; the background execution observes the
    /// exits and finalizes the task. Returns whether the task had anything to
    /// stop.
    pub async fn stop_task(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        reason: &str,
    ) -> bool {
        self.begin_stopping(state, account_id, task_id);
        if let Some(k) = &self.k8s {
            tracing::info!(task = %task_id, reason = %reason, "ecs task stop requested (k8s)");
            return k.stop_task(task_id).await;
        }
        // Its ExecuteCommand commands end with it, as Session Manager
        // sessions do when ECS stops the task.
        self.abort_execs(Some(task_id));
        let Some(list) = self.containers.read().get(task_id).cloned() else {
            return false;
        };
        if list.is_empty() {
            return false;
        }
        tokio::spawn(stop_in_waves(self.container_cli(), list));
        tracing::info!(task = %task_id, reason = %reason, "ecs task stop requested");
        true
    }

    /// [`Self::stop_task`], then wait until the containers have stopped and
    /// the background execution has finalized the task (STOPPED, its load
    /// balancer targets and ENI released), each container getting its full
    /// `stopTimeout`. For background teardown that drops the task record
    /// afterwards (stack deletion).
    pub async fn stop_task_and_wait(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        reason: &str,
    ) {
        let done = self.task_done.lock().get(task_id).cloned();
        let list = self
            .containers
            .read()
            .get(task_id)
            .cloned()
            .unwrap_or_default();
        let total_grace: u64 = match &self.k8s {
            Some(k) => k.grace_secs(task_id).await.unwrap_or(0),
            None => list.iter().map(|c| u64::from(c.stop_timeout)).sum(),
        };
        if self.k8s.is_some() {
            self.stop_task(state, account_id, task_id, reason).await;
        } else {
            self.begin_stopping(state, account_id, task_id);
            self.abort_execs(Some(task_id));
            stop_in_waves(self.container_cli(), list).await;
        }
        // The execution finishes within seconds of the last container exit;
        // a wedged daemon must not hang the caller.
        let budget = Duration::from_secs(total_grace) + CLI_CALL_TIMEOUT * 2;
        self.await_finalize(state, account_id, task_id, done, budget)
            .await;
    }

    /// Kill every container behind a task at once (SIGKILL; on Kubernetes,
    /// the Pod deleted with no grace period) and wait, a few seconds at
    /// most, for the execution to finalize it: for a reset, which must
    /// answer promptly and never waits out a `stopTimeout`. An execution
    /// still finalizing past that releases the task's network itself.
    pub async fn kill_task_and_wait(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
    ) {
        self.kill_and_wait(state, account_id, task_id, KILL_FINALIZE_WAIT)
            .await;
    }

    async fn kill_and_wait(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        wait: Duration,
    ) {
        let done = self.task_done.lock().get(task_id).cloned();
        self.begin_stopping(state, account_id, task_id);
        self.abort_execs(Some(task_id));
        if let Some(k) = &self.k8s {
            k.kill_task(task_id).await;
        } else {
            let list = self
                .containers
                .read()
                .get(task_id)
                .cloned()
                .unwrap_or_default();
            let mut kills = tokio::task::JoinSet::new();
            for c in list {
                let cli = self.container_cli();
                kills.spawn(async move {
                    let _ = cli
                        .output(["kill", &c.container_id], KILL_CALL_TIMEOUT)
                        .await;
                });
            }
            kills.join_all().await;
        }
        self.await_finalize(state, account_id, task_id, done, wait)
            .await;
    }

    /// Wait up to `budget` for the task's execution (`done`) to finalize it;
    /// release its network here when the execution never ran or didn't
    /// finish in time. The release runs exactly once either way.
    async fn await_finalize(
        &self,
        state: &SharedEcsState,
        account_id: &str,
        task_id: &str,
        done: Option<tokio::sync::watch::Receiver<bool>>,
        budget: Duration,
    ) {
        let orphaned = match done {
            // A live execution sees the stop and releases the network itself
            // when it finalizes, even past `budget`; only one that died
            // (its signal dropped unset) leaves it to us.
            Some(mut done) => matches!(
                tokio::time::timeout(budget, done.wait_for(|finished| *finished)).await,
                Ok(Err(_))
            ),
            // No execution in flight: it is over (and released the network
            // already), or it never started (a reset before `run_task`),
            // in which case what `begin_stopping` captured is released now.
            None => true,
        };
        if orphaned {
            let region = task_region(state, account_id, task_id);
            self.release_orphaned(state, account_id, &region, task_id);
        }
    }

    /// Introspection force-stop: request the stop as `StopTask` does (so a
    /// restart policy doesn't bring the containers back), kill the
    /// containers, and answer once the task is STOPPED (bounded by
    /// [`FORCE_STOP_WAIT`]). Returns false when no account has the task.
    pub async fn force_stop_task(
        &self,
        state: &SharedEcsState,
        task_id: &str,
        reason: &str,
    ) -> bool {
        let account_id = {
            let mut accounts = state.write();
            let Some((account_id, task)) = accounts
                .iter_mut()
                .find_map(|(id, s)| s.tasks.get_mut(task_id).map(|t| (id.to_string(), t)))
            else {
                return false;
            };
            request_task_stop(task, reason);
            account_id
        };
        self.kill_and_wait(state, &account_id, task_id, FORCE_STOP_WAIT)
            .await;
        true
    }
}

/// Budget for one `docker kill`.
const KILL_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a kill waits for the execution to finalize the task: a killed
/// container's exit is seen within one 200ms poll, and the finalize (logs,
/// `rm`, state) takes about a second more on a healthy daemon. Short enough
/// for an SDK's request timeout (5 seconds in the Python SDK).
const KILL_FINALIZE_WAIT: Duration = Duration::from_secs(3);

/// How long a force-stop waits, after the kill, for the task to be STOPPED:
/// it answers only once the task is finalized, and the kill is fast, so this
/// only covers the engine removing the containers and capturing their logs
/// on a slow host (Podman, a loaded CI runner).
const FORCE_STOP_WAIT: Duration = Duration::from_secs(60);

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(name: &str, deps: &[&str]) -> TaskContainerHandle {
        TaskContainerHandle {
            name: name.into(),
            container_id: format!("id-{name}"),
            stop_timeout: 30,
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
        }
    }

    #[test]
    fn independent_containers_stop_in_one_wave() {
        let c = [handle("a", &[]), handle("b", &[]), handle("c", &[])];
        assert_eq!(stop_waves(&c), vec![vec![0, 1, 2]]);
    }

    #[test]
    fn dependents_stop_before_their_dependencies() {
        // app depends on db and cache; db depends on init.
        let c = [
            handle("init", &[]),
            handle("db", &["init"]),
            handle("cache", &[]),
            handle("app", &["db", "cache"]),
            handle("sidecar", &[]),
        ];
        assert_eq!(stop_waves(&c), vec![vec![3, 4], vec![1, 2], vec![0]]);
    }

    #[test]
    fn unknown_dependencies_and_cycles_still_stop_everything() {
        let c = [handle("a", &["ghost"]), handle("b", &[])];
        assert_eq!(stop_waves(&c), vec![vec![0, 1]]);
        let cyc = [
            handle("a", &["b"]),
            handle("b", &["a"]),
            handle("c", &["a"]),
        ];
        assert_eq!(stop_waves(&cyc), vec![vec![2], vec![0, 1]]);
        assert!(stop_waves(&[]).is_empty());
    }

    fn state_with(task: crate::state::Task) -> SharedEcsState {
        let mut accounts: fakecloud_core::multi_account::MultiAccountState<crate::state::EcsState> =
            fakecloud_core::multi_account::MultiAccountState::new(
                "000000000000",
                "us-east-1",
                "http://localhost:4566",
            );
        let id = task.task_id.clone();
        accounts
            .get_or_create("000000000000")
            .tasks
            .insert(id, task);
        Arc::new(parking_lot::RwLock::new(accounts))
    }

    fn status(state: &SharedEcsState) -> (String, String) {
        let accounts = state.read();
        let t = &accounts.get("000000000000").unwrap().tasks["t1"];
        (t.last_status.clone(), t.desired_status.clone())
    }

    #[test]
    fn a_running_task_moves_to_stopping_and_deprovisioning() {
        let rt = EcsRuntime::bare_for_tests();
        let mut task = crate::runtime::tests::make_task("t1");
        task.last_status = "RUNNING".into();
        super::super::eni::attach_eni(
            &mut task,
            "att-1",
            "eni-0123",
            "subnet-1",
            "10.0.0.9".parse().unwrap(),
            "02:00:00:00:00:01",
        );
        let state = state_with(task);
        rt.begin_stopping(&state, "000000000000", "t1");
        assert_eq!(status(&state), ("STOPPING".into(), "STOPPED".into()));
        {
            let accounts = state.read();
            assert!(accounts.get("000000000000").unwrap().tasks["t1"]
                .stopping_at
                .is_some());
        }
        // Stopping twice is a no-op.
        rt.begin_stopping(&state, "000000000000", "t1");
        assert_eq!(rt.teardowns.lock().len(), 1);
        rt.deprovision(&state, "000000000000", "us-east-1", "t1");
        assert_eq!(status(&state).0, "DEPROVISIONING");
        assert!(rt.teardowns.lock().is_empty(), "teardown runs once");
    }

    #[test]
    fn a_stopped_task_keeps_its_status() {
        let rt = EcsRuntime::bare_for_tests();
        let mut task = crate::runtime::tests::make_task("t1");
        task.last_status = "STOPPED".into();
        let state = state_with(task);
        rt.begin_stopping(&state, "000000000000", "t1");
        assert_eq!(status(&state).0, "STOPPED");
        // Nothing captured from the stale record (its targets / ENI IP may
        // belong to a replacement now).
        assert!(rt.teardowns.lock().is_empty());
        assert!(!rt.stopping_began("t1"));
    }

    fn running_awsvpc_task(id: &str, ip: &str) -> crate::state::Task {
        let mut task = crate::runtime::tests::make_task(id);
        task.last_status = "RUNNING".into();
        super::super::eni::attach_eni(
            &mut task,
            "att-1",
            &format!("eni-{id}"),
            "subnet-1",
            ip.parse().unwrap(),
            "02:00:00:00:00:01",
        );
        task
    }

    /// A kill that gives up waiting while the execution is still alive (an
    /// awsvpc task still pulling) releases nothing and leaves the release to
    /// the execution, which by then holds the ENI and route it creates.
    #[tokio::test]
    async fn a_live_execution_keeps_its_own_release() {
        use fakecloud_core::dataplane::{register_target, registered_target, Endpoint};
        let acct = "000000000000";
        let rt = EcsRuntime::bare_for_tests();
        let mut task = crate::runtime::tests::make_task("t3");
        task.last_status = "PENDING".into();
        let state = state_with(task);
        let done = rt.track_task_execution("t3");
        let rx = rt.task_done.lock().get("t3").cloned();
        rt.begin_stopping(&state, acct, "t3");
        rt.await_finalize(&state, acct, "t3", rx, Duration::from_millis(50))
            .await;
        assert!(
            !rt.released.lock().contains("t3"),
            "fallback closed the release"
        );
        // The execution then attaches its ENI and publishes its ports...
        {
            let mut accounts = state.write();
            let t = accounts.get_mut(acct).unwrap().tasks.get_mut("t3").unwrap();
            super::super::eni::attach_eni(
                t,
                "att-3",
                "eni-t3",
                "subnet-1",
                "10.9.0.11".parse().unwrap(),
                "02:00:00:00:00:03",
            );
        }
        register_target(acct, "10.9.0.11", 80, Endpoint::new("127.0.0.1", 40011));
        // ...and its finalize releases them.
        rt.deprovision(&state, acct, "us-east-1", "t3");
        rt.task_execution_finished("t3", &done);
        assert!(registered_target(acct, "10.9.0.11", 80).is_none());
    }

    /// An execution that died without finalizing leaves the release to the
    /// waiter.
    #[tokio::test]
    async fn a_dead_execution_is_released_by_the_waiter() {
        use fakecloud_core::dataplane::{register_target, registered_target, Endpoint};
        let acct = "000000000000";
        let rt = EcsRuntime::bare_for_tests();
        let state = state_with(running_awsvpc_task("t4", "10.9.0.12"));
        register_target(acct, "10.9.0.12", 80, Endpoint::new("127.0.0.1", 40012));
        let done = rt.track_task_execution("t4");
        let rx = rt.task_done.lock().get("t4").cloned();
        rt.begin_stopping(&state, acct, "t4");
        drop(done);
        rt.await_finalize(&state, acct, "t4", rx, Duration::from_secs(5))
            .await;
        assert!(registered_target(acct, "10.9.0.12", 80).is_none());
    }

    /// A task's network is released once: a second release (a late
    /// finalize after a reset's fallback) must not remove the route of a
    /// replacement that reused the ENI IP.
    #[test]
    fn network_is_released_exactly_once() {
        use fakecloud_core::dataplane::{register_target, registered_target, Endpoint};
        let acct = "000000000000";
        let rt = EcsRuntime::bare_for_tests();
        let state = state_with(running_awsvpc_task("t1", "10.9.0.7"));
        register_target(acct, "10.9.0.7", 80, Endpoint::new("127.0.0.1", 40001));
        rt.begin_stopping(&state, acct, "t1");
        rt.finish_teardown(&state, acct, "us-east-1", "t1");
        assert!(registered_target(acct, "10.9.0.7", 80).is_none());
        // A replacement reuses the IP; the old task's late release is a no-op.
        register_target(acct, "10.9.0.7", 80, Endpoint::new("127.0.0.1", 40002));
        rt.finish_teardown(&state, acct, "us-east-1", "t1");
        rt.deprovision(&state, acct, "us-east-1", "t1");
        assert!(registered_target(acct, "10.9.0.7", 80).is_some());
        assert!(rt.stopping_began("t1"));
    }

    /// A reset that drops a task before its execution ever started still
    /// releases what the task held (its precreated ENI).
    #[tokio::test]
    async fn kill_releases_a_task_that_never_ran() {
        use fakecloud_core::dataplane::{register_target, registered_target, Endpoint};
        let acct = "000000000000";
        let rt = EcsRuntime::bare_for_tests();
        let mut task = running_awsvpc_task("t2", "10.9.0.8");
        task.last_status = "PENDING".into();
        task.task_id = "t2".into();
        let state = state_with(task);
        register_target(acct, "10.9.0.8", 80, Endpoint::new("127.0.0.1", 40003));
        rt.begin_stopping(&state, acct, "t2");
        state.write().get_mut(acct).unwrap().tasks.remove("t2");
        let started = std::time::Instant::now();
        rt.kill_task_and_wait(&state, acct, "t2").await;
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(rt.teardowns.lock().is_empty());
        assert!(registered_target(acct, "10.9.0.8", 80).is_none());
    }

    #[test]
    fn a_scheduler_stop_keeps_its_stop_code() {
        let mut task = crate::runtime::tests::make_task("t1");
        task.stop_code = Some("ServiceSchedulerInitiated".into());
        task.stopped_reason = Some("Scaling activity initiated by (deployment ecs-svc/1)".into());
        let state = state_with(task);
        assert_eq!(
            requested_stop(&state, "000000000000", "t1"),
            Some((
                "ServiceSchedulerInitiated",
                Some("Scaling activity initiated by (deployment ecs-svc/1)".into())
            ))
        );
        let mut exited = crate::runtime::tests::make_task("t1");
        exited.stop_code = Some("EssentialContainerExited".into());
        assert_eq!(
            requested_stop(&state_with(exited), "000000000000", "t1"),
            None
        );
    }

    /// The network teardown is captured when the task starts stopping, so
    /// it still runs after the task record is dropped (reset, stack delete).
    #[test]
    fn teardown_survives_the_task_record() {
        let rt = EcsRuntime::bare_for_tests();
        let mut task = crate::runtime::tests::make_task("t1");
        task.last_status = "RUNNING".into();
        super::super::eni::attach_eni(
            &mut task,
            "att-1",
            "eni-0123",
            "subnet-1",
            "10.0.0.9".parse().unwrap(),
            "02:00:00:00:00:01",
        );
        let state = state_with(task);
        rt.begin_stopping(&state, "000000000000", "t1");
        state
            .write()
            .get_mut("000000000000")
            .unwrap()
            .tasks
            .remove("t1");
        {
            let teardowns = rt.teardowns.lock();
            let t = teardowns.get("t1").expect("captured");
            assert_eq!(t.eni_id.as_deref(), Some("eni-0123"));
            assert_eq!(t.eni_ip.as_deref(), Some("10.0.0.9"));
            assert_eq!(t.region, "us-east-1");
        }
        rt.finish_teardown(&state, "000000000000", "us-east-1", "t1");
        assert!(rt.teardowns.lock().is_empty());
    }

    /// Dependents stop first; containers with no dependency between them
    /// stop together, each with its own stopTimeout.
    #[tokio::test]
    async fn waves_stop_in_order_and_in_parallel() {
        let (dir, cli) = fakecloud_core::test_support::fake_cli(
            "cli",
            r#"[ "$1" = warmup ] && exit 0
d=$(dirname "$0")
echo "start $4 $3" >> "$d/log"
sleep 1
echo "end $4" >> "$d/log""#,
        );
        let rt = EcsRuntime::with_cli_for_tests(&cli);
        let mut db = handle("db", &[]);
        db.stop_timeout = 7;
        let containers = vec![db, handle("cache", &[]), handle("app", &["db", "cache"])];
        let started = std::time::Instant::now();
        stop_in_waves(rt.container_cli(), containers).await;
        let elapsed = started.elapsed();
        let log = std::fs::read_to_string(dir.path().join("log")).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines[0], "start id-app 30", "{log}");
        assert_eq!(lines[1], "end id-app", "{log}");
        // db and cache both started before either finished.
        assert!(
            lines[2].starts_with("start ") && lines[3].starts_with("start "),
            "{log}"
        );
        assert!(lines.contains(&"start id-db 7"), "{log}");
        assert!(lines.contains(&"start id-cache 30"), "{log}");
        assert!(elapsed < Duration::from_millis(2900), "took {elapsed:?}");
    }

    #[test]
    fn request_task_stop_sets_what_stop_task_sets_once() {
        let mut task = crate::runtime::tests::make_task("t1");
        request_task_stop(&mut task, "first");
        assert_eq!(task.desired_status, "STOPPED");
        assert_eq!(task.stop_code.as_deref(), Some("UserInitiated"));
        assert_eq!(task.stopped_reason.as_deref(), Some("first"));
        let at = task.stopping_at.expect("stoppingAt");
        request_task_stop(&mut task, "second");
        assert_eq!(task.stopped_reason.as_deref(), Some("first"));
        assert_eq!(task.stopping_at, Some(at));
    }
}
