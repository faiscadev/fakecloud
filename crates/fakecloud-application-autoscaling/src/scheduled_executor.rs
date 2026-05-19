//! Background executor that fires Application Auto Scaling
//! `ScheduledAction`s when their `Schedule` expression is due.
//!
//! AWS supports two expression forms here:
//! - `at(yyyy-mm-ddThh:mm:ss)` — fires once when wall-clock catches up
//! - `cron(min hour dom month dow year)` — recurring; the six-field
//!   cron grammar matches EventBridge Scheduler. We support wildcards
//!   (`*` / `?`) and single numeric values per field; ranges/lists/step
//!   values are rejected so unsupported schedules silently never fire
//!   instead of firing every minute.
//!
//! When an action is due, the executor:
//!   1. Mutates the matching `ScalableTarget` `min_capacity` /
//!      `max_capacity` per the action's `ScalableTargetAction`.
//!   2. Calls into the configured cross-service hook to apply the new
//!      bounds on the underlying resource (DynamoDB capacity today;
//!      ECS/RDS hooks slot in alongside as those are wired).
//!   3. Appends a `ScalingActivity` row so `DescribeScalingActivities`
//!      surfaces the firing.
//!   4. Records `last_fired_at` so the next tick within the same
//!      minute doesn't re-fire a `cron(* * * * ? *)` schedule.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use uuid::Uuid;

use crate::hooks::{DynamoDbCapacityHook, EcsServiceHook};
use crate::state::{ScalingActivity, ScheduledAction, SharedApplicationAutoScalingState};
use crate::ticker::{
    ddb_table_from_resource_public, ecs_service_from_resource, DDB_READ_DIM, DDB_WRITE_DIM,
};

/// Background loop driver. Construction wires the shared state and the
/// hooks that apply scaling decisions on real resources.
pub struct ScheduledActionExecutor {
    state: SharedApplicationAutoScalingState,
    ddb_hook: Arc<dyn DynamoDbCapacityHook>,
    ecs_hook: Option<Arc<dyn EcsServiceHook>>,
    region: String,
    interval: Duration,
}

impl ScheduledActionExecutor {
    pub fn new(
        state: SharedApplicationAutoScalingState,
        ddb_hook: Arc<dyn DynamoDbCapacityHook>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            state,
            ddb_hook,
            ecs_hook: None,
            region: region.into(),
            interval: Duration::from_secs(30),
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_ecs_hook(mut self, hook: Arc<dyn EcsServiceHook>) -> Self {
        self.ecs_hook = Some(hook);
        self
    }

    /// Drive a single evaluation. Exposed for the admin tick endpoint
    /// and tests so callers don't have to wait on the wall-clock
    /// interval. Returns the number of actions that fired this tick.
    pub fn tick_once(&self) -> usize {
        self.tick_at(Utc::now())
    }

    /// Same as `tick_once`, but evaluates against an explicit `now`.
    /// Useful for unit tests that need to pin the wall clock.
    pub fn tick_at(&self, now: DateTime<Utc>) -> usize {
        // Snapshot due actions under a read lock so we can release it
        // before invoking cross-service hooks (which may take other
        // locks downstream).
        struct Job {
            account_id: String,
            action_key: (String, String, String, String),
            action: ScheduledAction,
        }
        let mut jobs: Vec<Job> = Vec::new();
        {
            let guard = self.state.read();
            for (account_id, account) in guard.accounts.iter() {
                for (key, action) in account.scheduled_actions.iter() {
                    if !is_due(action, now) {
                        continue;
                    }
                    // Skip when the linked target has scheduled
                    // scaling explicitly suspended.
                    let target_key = (
                        action.service_namespace.clone(),
                        action.resource_id.clone(),
                        action.scalable_dimension.clone().unwrap_or_default(),
                    );
                    if let Some(target) = account.scalable_targets.get(&target_key) {
                        if target
                            .suspended_state
                            .as_ref()
                            .and_then(|s| s.scheduled_scaling_suspended)
                            .unwrap_or(false)
                        {
                            continue;
                        }
                    } else {
                        continue;
                    }
                    jobs.push(Job {
                        account_id: account_id.clone(),
                        action_key: key.clone(),
                        action: action.clone(),
                    });
                }
            }
        }

        let mut fired = 0;
        for job in jobs {
            if self.fire_action(&job.account_id, &job.action_key, &job.action, now) {
                fired += 1;
            }
        }
        fired
    }

    /// Long-running loop that ticks at `self.interval` until the
    /// process exits. Skips the immediate first tick — the server is
    /// still wiring services up.
    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            let _ = self.tick_once();
        }
    }

    fn fire_action(
        &self,
        account_id: &str,
        action_key: &(String, String, String, String),
        action: &ScheduledAction,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(target_action) = action.scalable_target_action.as_ref() else {
            // Without a ScalableTargetAction there's nothing to apply;
            // mark fired so we don't loop. AWS rejects this at create
            // time, but defensive here for forward-compat.
            self.record_fire(
                account_id,
                action_key,
                action,
                now,
                "Failed",
                Some("ScheduledAction has no ScalableTargetAction".to_string()),
            );
            return false;
        };

        // Snapshot existing target bounds so we know what we're moving from.
        let target_key = (
            action.service_namespace.clone(),
            action.resource_id.clone(),
            action.scalable_dimension.clone().unwrap_or_default(),
        );
        let (prev_min, prev_max) = {
            let guard = self.state.read();
            let Some(account) = guard.accounts.get(account_id) else {
                return false;
            };
            let Some(target) = account.scalable_targets.get(&target_key) else {
                return false;
            };
            (target.min_capacity, target.max_capacity)
        };

        let new_min = target_action.min_capacity.unwrap_or(prev_min);
        let new_max = target_action.max_capacity.unwrap_or(prev_max);
        // AWS rejects min > max with a `ValidationException` at PUT
        // time, but the bounds may have drifted since (someone could
        // re-RegisterScalableTarget with tighter bounds). Clamp rather
        // than fail-loudly so the executor stays robust.
        let (new_min, new_max) = if new_min > new_max {
            (new_max, new_max)
        } else {
            (new_min, new_max)
        };

        // Apply across-service. Today only DynamoDB has a wired hook.
        // Other namespaces (ECS, RDS, Lambda, Aurora) update the
        // ScalableTarget bounds and emit a ScalingActivity but don't
        // mutate the underlying resource until those hooks exist.
        let apply_result = self.apply_to_resource(account_id, action, new_min, new_max);
        let (status, message) = match apply_result {
            Ok(()) => ("Successful".to_string(), None),
            Err(err) => ("Failed".to_string(), Some(err)),
        };

        // Mutate the ScalableTarget bounds + record the activity + bump
        // last_fired_at under a single write lock so observers see a
        // consistent view.
        let mut guard = self.state.write();
        let account = guard.accounts.entry(account_id.to_string()).or_default();
        if let Some(target) = account.scalable_targets.get_mut(&target_key) {
            target.min_capacity = new_min;
            target.max_capacity = new_max;
        }
        if let Some(stored) = account.scheduled_actions.get_mut(action_key) {
            stored.last_fired_at = Some(now);
        }
        account.scaling_activities.push(ScalingActivity {
            activity_id: Uuid::new_v4().to_string(),
            service_namespace: action.service_namespace.clone(),
            resource_id: action.resource_id.clone(),
            scalable_dimension: action.scalable_dimension.clone().unwrap_or_default(),
            description: format!(
                "Setting min capacity to {new_min} and max capacity to {new_max} for {res} from scheduled action {name}",
                res = action.resource_id,
                name = action.scheduled_action_name,
            ),
            cause: format!(
                "Scheduled action {name} fired (schedule: {schedule})",
                name = action.scheduled_action_name,
                schedule = action.schedule,
            ),
            start_time: now,
            end_time: Some(now),
            status_code: status.clone(),
            status_message: message,
            details: None,
            not_scaled_reasons: Vec::new(),
        });

        status == "Successful"
    }

    fn apply_to_resource(
        &self,
        account_id: &str,
        action: &ScheduledAction,
        new_min: i32,
        new_max: i32,
    ) -> Result<(), String> {
        match action.service_namespace.as_str() {
            "dynamodb" => {
                let Some(dimension) = action.scalable_dimension.as_deref() else {
                    return Err(
                        "ScalableDimension is required for dynamodb scheduled actions".to_string(),
                    );
                };
                let Some(table) = ddb_table_from_resource_public(&action.resource_id) else {
                    return Err(format!(
                        "Cannot derive DynamoDB table from resource_id {}",
                        action.resource_id
                    ));
                };
                // Pull current capacity so we know whether to bump up
                // or leave alone. AWS scheduled actions move capacity
                // to ScalableTargetAction.MinCapacity if the current
                // value is below it (and analogously for the max).
                let Some((read_cur, write_cur)) =
                    self.ddb_hook
                        .current_capacity(account_id, &self.region, table)
                else {
                    return Err(format!(
                        "DynamoDB table {table} not found or not on PROVISIONED billing"
                    ));
                };
                let current = match dimension {
                    DDB_READ_DIM => read_cur,
                    DDB_WRITE_DIM => write_cur,
                    other => {
                        return Err(format!("Unsupported DynamoDB scalable dimension {other}"));
                    }
                };
                // Pin capacity into the new [min, max] window.
                let mut desired = current;
                if desired < new_min as i64 {
                    desired = new_min as i64;
                }
                if desired > new_max as i64 {
                    desired = new_max as i64;
                }
                if desired == current {
                    return Ok(());
                }
                let (read, write) = match dimension {
                    DDB_READ_DIM => (Some(desired), None),
                    DDB_WRITE_DIM => (None, Some(desired)),
                    _ => unreachable!("dimension validated above"),
                };
                self.ddb_hook
                    .set_capacity(account_id, &self.region, table, read, write)
            }
            "ecs" => {
                let Some((cluster, service)) = ecs_service_from_resource(&action.resource_id)
                else {
                    return Err(format!(
                        "Cannot derive ECS cluster/service from resource_id {}",
                        action.resource_id
                    ));
                };
                let Some(hook) = self.ecs_hook.as_ref() else {
                    return Ok(());
                };
                let Some(current) =
                    hook.current_desired_count(account_id, &self.region, cluster, service)
                else {
                    return Err(format!(
                        "ECS service {service} not found in cluster {cluster}"
                    ));
                };
                // Pin desired count into the new [min, max] window.
                let mut desired = current as i64;
                if desired < new_min as i64 {
                    desired = new_min as i64;
                }
                if desired > new_max as i64 {
                    desired = new_max as i64;
                }
                if desired == current as i64 {
                    return Ok(());
                }
                hook.set_desired_count(account_id, &self.region, cluster, service, desired as i32)
            }
            // Other namespaces don't yet have a cross-service apply
            // hook in this crate. Updating the scalable target bounds
            // is still useful — when those hooks land they'll observe
            // the new bounds on the next reconciliation.
            _ => Ok(()),
        }
    }

    fn record_fire(
        &self,
        account_id: &str,
        action_key: &(String, String, String, String),
        action: &ScheduledAction,
        now: DateTime<Utc>,
        status: &str,
        message: Option<String>,
    ) {
        let mut guard = self.state.write();
        let account = guard.accounts.entry(account_id.to_string()).or_default();
        if let Some(stored) = account.scheduled_actions.get_mut(action_key) {
            stored.last_fired_at = Some(now);
        }
        account.scaling_activities.push(ScalingActivity {
            activity_id: Uuid::new_v4().to_string(),
            service_namespace: action.service_namespace.clone(),
            resource_id: action.resource_id.clone(),
            scalable_dimension: action.scalable_dimension.clone().unwrap_or_default(),
            description: format!(
                "Scheduled action {name} could not fire",
                name = action.scheduled_action_name,
            ),
            cause: format!(
                "Scheduled action {name} (schedule: {schedule})",
                name = action.scheduled_action_name,
                schedule = action.schedule,
            ),
            start_time: now,
            end_time: Some(now),
            status_code: status.to_string(),
            status_message: message,
            details: None,
            not_scaled_reasons: Vec::new(),
        });
    }
}

/// Parse a `Schedule` expression and decide whether it's due.
///
/// Honours the action's `start_time` / `end_time` window: the action
/// is silent before `start_time` and after `end_time`. Re-fire dedup
/// for cron schedules lives in `last_fired_at` — we never re-fire
/// inside the same minute.
fn is_due(action: &ScheduledAction, now: DateTime<Utc>) -> bool {
    if let Some(start) = action.start_time {
        if now < start {
            return false;
        }
    }
    if let Some(end) = action.end_time {
        if now > end {
            return false;
        }
    }
    let schedule = action.schedule.trim();
    if let Some(inner) = schedule
        .strip_prefix("at(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return is_at_due(inner, action.last_fired_at, now);
    }
    if let Some(inner) = schedule
        .strip_prefix("cron(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return is_cron_due(inner, action.timezone.as_deref(), action.last_fired_at, now);
    }
    // Unknown schedule shape — never fires. Matches AWS behavior of
    // silently skipping schedules with malformed expressions rather
    // than blowing up the executor loop.
    false
}

fn is_at_due(inner: &str, last_fired: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    if last_fired.is_some() {
        return false;
    }
    let dt = match NaiveDateTime::parse_from_str(inner.trim(), "%Y-%m-%dT%H:%M:%S") {
        Ok(dt) => Utc.from_utc_datetime(&dt),
        Err(_) => return false,
    };
    now >= dt
}

fn is_cron_due(
    inner: &str,
    tz: Option<&str>,
    last_fired: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 6 {
        return false;
    }
    let minute = parse_cron_field(parts[0]);
    let hour = parse_cron_field(parts[1]);
    let dom = parse_cron_field(parts[2]);
    let month = parse_cron_field(parts[3]);
    let dow = parse_cron_field(parts[4]);
    let (Some(minute), Some(hour), Some(dom), Some(month), Some(dow)) =
        (minute, hour, dom, month, dow)
    else {
        return false;
    };
    let (m, h, d, mo, w) = match tz.and_then(|s| s.parse::<chrono_tz::Tz>().ok()) {
        Some(tz) => {
            let local = now.with_timezone(&tz);
            (
                local.minute(),
                local.hour(),
                local.day(),
                local.month(),
                local.weekday().num_days_from_sunday(),
            )
        }
        None => (
            now.minute(),
            now.hour(),
            now.day(),
            now.month(),
            now.weekday().num_days_from_sunday(),
        ),
    };
    if !field_matches(&minute, m)
        || !field_matches(&hour, h)
        || !field_matches(&dom, d)
        || !field_matches(&month, mo)
        || !field_matches(&dow, w)
    {
        return false;
    }
    // Per-minute dedupe: never re-fire within the same wall-clock
    // minute. Match scheduler semantics — `last_fired_at` is set
    // immediately after a fire, so the very next tick within that
    // minute must skip.
    if let Some(last) = last_fired {
        if same_minute(last, now) {
            return false;
        }
    }
    true
}

#[derive(Clone, Copy)]
enum CronField {
    Any,
    Value(u32),
}

fn parse_cron_field(s: &str) -> Option<CronField> {
    if s == "*" || s == "?" {
        return Some(CronField::Any);
    }
    s.parse::<u32>().ok().map(CronField::Value)
}

fn field_matches(f: &CronField, actual: u32) -> bool {
    match f {
        CronField::Any => true,
        CronField::Value(v) => *v == actual,
    }
}

fn same_minute(a: DateTime<Utc>, b: DateTime<Utc>) -> bool {
    a.year() == b.year()
        && a.month() == b.month()
        && a.day() == b.day()
        && a.hour() == b.hour()
        && a.minute() == b.minute()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        ApplicationAutoScalingAccounts, ScalableTarget, ScalableTargetAction, ScheduledAction,
    };
    use parking_lot::RwLock;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Mutex;

    struct StubDdb {
        read: AtomicI64,
        write: AtomicI64,
        calls: Mutex<Vec<(Option<i64>, Option<i64>)>>,
    }
    impl DynamoDbCapacityHook for StubDdb {
        fn current_capacity(&self, _a: &str, _r: &str, _t: &str) -> Option<(i64, i64)> {
            Some((
                self.read.load(Ordering::Relaxed),
                self.write.load(Ordering::Relaxed),
            ))
        }
        fn set_capacity(
            &self,
            _a: &str,
            _r: &str,
            _t: &str,
            read: Option<i64>,
            write: Option<i64>,
        ) -> Result<(), String> {
            if let Some(r) = read {
                self.read.store(r, Ordering::Relaxed);
            }
            if let Some(w) = write {
                self.write.store(w, Ordering::Relaxed);
            }
            self.calls.lock().unwrap().push((read, write));
            Ok(())
        }
    }

    fn fixture() -> (SharedApplicationAutoScalingState, Arc<StubDdb>) {
        let state: SharedApplicationAutoScalingState =
            Arc::new(RwLock::new(ApplicationAutoScalingAccounts::new()));
        {
            let mut guard = state.write();
            let acct = guard
                .accounts
                .entry("123456789012".to_string())
                .or_default();
            acct.scalable_targets.insert(
                (
                    "dynamodb".to_string(),
                    "table/orders".to_string(),
                    DDB_READ_DIM.to_string(),
                ),
                ScalableTarget {
                    arn: "arn:aws:application-autoscaling:::scalable-target/abc".to_string(),
                    service_namespace: "dynamodb".to_string(),
                    resource_id: "table/orders".to_string(),
                    scalable_dimension: DDB_READ_DIM.to_string(),
                    min_capacity: 1,
                    max_capacity: 100,
                    role_arn: "role".to_string(),
                    creation_time: Utc::now(),
                    suspended_state: None,
                    predicted_capacity: None,
                },
            );
        }
        let ddb = Arc::new(StubDdb {
            read: AtomicI64::new(2),
            write: AtomicI64::new(2),
            calls: Mutex::new(Vec::new()),
        });
        (state, ddb)
    }

    fn put_action(
        state: &SharedApplicationAutoScalingState,
        name: &str,
        schedule: &str,
        min: Option<i32>,
        max: Option<i32>,
    ) {
        let mut guard = state.write();
        let acct = guard.accounts.get_mut("123456789012").unwrap();
        acct.scheduled_actions.insert(
            (
                "dynamodb".to_string(),
                "table/orders".to_string(),
                DDB_READ_DIM.to_string(),
                name.to_string(),
            ),
            ScheduledAction {
                arn: format!("arn:aws:autoscaling:::scheduledAction:{name}"),
                scheduled_action_name: name.to_string(),
                service_namespace: "dynamodb".to_string(),
                resource_id: "table/orders".to_string(),
                scalable_dimension: Some(DDB_READ_DIM.to_string()),
                schedule: schedule.to_string(),
                timezone: None,
                start_time: None,
                end_time: None,
                scalable_target_action: Some(ScalableTargetAction {
                    min_capacity: min,
                    max_capacity: max,
                }),
                creation_time: Utc::now(),
                last_fired_at: None,
            },
        );
    }

    #[test]
    fn cron_every_minute_fires_and_bumps_capacity() {
        let (state, ddb) = fixture();
        put_action(&state, "warm", "cron(* * * * ? *)", Some(10), Some(50));
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        let fired = exec.tick_once();
        assert_eq!(fired, 1);
        assert_eq!(ddb.read.load(Ordering::Relaxed), 10, "read bumped to min");
        // Second tick same minute must be a no-op.
        let now = state
            .read()
            .accounts
            .get("123456789012")
            .unwrap()
            .scheduled_actions
            .values()
            .next()
            .unwrap()
            .last_fired_at
            .unwrap();
        let fired_again = exec.tick_at(now);
        assert_eq!(fired_again, 0, "cron must not re-fire within same minute");
    }

    #[test]
    fn cron_at_specific_minute_does_not_fire_off_minute() {
        let (state, ddb) = fixture();
        // Schedule for minute 45 only.
        put_action(&state, "off-minute", "cron(45 * * * ? *)", Some(20), None);
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 30, 0).unwrap();
        assert_eq!(exec.tick_at(now), 0, "must not fire off-minute");
        let later = Utc.with_ymd_and_hms(2026, 1, 1, 10, 45, 30).unwrap();
        assert_eq!(exec.tick_at(later), 1, "must fire on minute 45");
    }

    #[test]
    fn at_expression_fires_once() {
        let (state, ddb) = fixture();
        put_action(
            &state,
            "one-shot",
            "at(2026-01-01T12:00:00)",
            Some(8),
            Some(80),
        );
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        let before = Utc.with_ymd_and_hms(2026, 1, 1, 11, 59, 59).unwrap();
        assert_eq!(exec.tick_at(before), 0);
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 1).unwrap();
        assert_eq!(exec.tick_at(after), 1);
        // Re-tick: must not re-fire because last_fired_at is now Some.
        let later = Utc.with_ymd_and_hms(2026, 1, 1, 13, 0, 0).unwrap();
        assert_eq!(exec.tick_at(later), 0);
    }

    #[test]
    fn end_time_silences_action() {
        let (state, ddb) = fixture();
        put_action(&state, "expired", "cron(* * * * ? *)", Some(10), None);
        // Mutate end_time to be in the past.
        {
            let mut guard = state.write();
            let acct = guard.accounts.get_mut("123456789012").unwrap();
            for action in acct.scheduled_actions.values_mut() {
                action.end_time = Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
            }
        }
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        assert_eq!(exec.tick_once(), 0, "end_time in past must silence");
    }

    #[test]
    fn unparseable_schedule_never_fires() {
        let (state, ddb) = fixture();
        put_action(&state, "bad", "every minute", Some(10), None);
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        assert_eq!(exec.tick_once(), 0);
    }

    #[test]
    fn updates_scalable_target_bounds_on_fire() {
        let (state, ddb) = fixture();
        put_action(&state, "tighten", "cron(* * * * ? *)", Some(15), Some(40));
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        assert_eq!(exec.tick_once(), 1);
        let guard = state.read();
        let target = guard
            .accounts
            .get("123456789012")
            .unwrap()
            .scalable_targets
            .get(&(
                "dynamodb".to_string(),
                "table/orders".to_string(),
                DDB_READ_DIM.to_string(),
            ))
            .unwrap();
        assert_eq!(target.min_capacity, 15);
        assert_eq!(target.max_capacity, 40);
    }

    #[test]
    fn suspended_scheduled_scaling_skips_fire() {
        let (state, ddb) = fixture();
        put_action(&state, "warm", "cron(* * * * ? *)", Some(10), None);
        // Mark scheduled scaling as suspended on the target.
        {
            let mut guard = state.write();
            let acct = guard.accounts.get_mut("123456789012").unwrap();
            for target in acct.scalable_targets.values_mut() {
                target.suspended_state = Some(crate::state::SuspendedState {
                    dynamic_scaling_in_suspended: None,
                    dynamic_scaling_out_suspended: None,
                    scheduled_scaling_suspended: Some(true),
                });
            }
        }
        let exec = ScheduledActionExecutor::new(state.clone(), ddb.clone(), "us-east-1");
        assert_eq!(exec.tick_once(), 0);
    }
}
