//! Background firing loop for EventBridge Scheduler.
//!
//! A single tokio task scans every enabled schedule once per second,
//! fires the ones whose expressions are due, updates `last_fired`, and
//! handles `ActionAfterCompletion=DELETE` for one-shot `at(...)` runs.
//! Delivery itself lives in `delivery.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, Timelike, Utc};

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_persistence::SnapshotStore;
use tokio::sync::Mutex as AsyncMutex;

use crate::delivery::{deliver_target, route_to_dlq};
use crate::expr::{self, Expr};
use crate::service::save_scheduler_snapshot;
use crate::state::{ScheduleKey, SharedSchedulerState};

pub struct Ticker {
    state: SharedSchedulerState,
    delivery: Arc<DeliveryBus>,
    /// Persist hook. The ticker mutates schedule state directly (sets
    /// `last_fired`, deletes one-shot `at(...)` runs on completion) outside the
    /// service's action-dispatch path that is otherwise the only thing that
    /// snapshots -- so without writing through here a fired one-shot reappears,
    /// and rate-rule timing resets, after a restart (bug-audit 2026-06-20, 0.A5).
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

/// Bookkeeping for an in-flight retry: how many delivery attempts the
/// scheduler has already made for this fire, the last error, and the
/// timestamp of the original due moment so MaximumEventAgeInSeconds can
/// retire the attempt.
#[derive(Clone, Debug)]
struct RetryState {
    attempts: i64,
    first_due: DateTime<Utc>,
    last_error: String,
}

/// Pending fire deferred by FlexibleTimeWindow: the actual delivery is
/// pushed out by `0..=max_window_in_minutes*60` seconds from when the
/// schedule first became due, picked deterministically per schedule
/// per-window so the perturbation is stable across ticks.
#[derive(Clone, Copy, Debug)]
struct PendingFire {
    fire_at: DateTime<Utc>,
}

impl Ticker {
    pub fn new(state: SharedSchedulerState, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Wire the snapshot store so each tick that mutates schedule state is
    /// written through to disk (see the `snapshot_store` field).
    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // (hour, minute) pair of the last cron fire, keyed by schedule
        // identity — keeps cron schedules from firing multiple times
        // inside the same minute when the 1s tick runs 60+ times.
        let mut cron_last_minute: HashMap<(String, ScheduleKey), CronFireStamp> = HashMap::new();
        let mut pending_fires: HashMap<(String, ScheduleKey), PendingFire> = HashMap::new();
        let mut retries: HashMap<(String, ScheduleKey), RetryState> = HashMap::new();
        loop {
            interval.tick().await;
            self.tick_and_persist(&mut cron_last_minute, &mut pending_fires, &mut retries)
                .await;
        }
    }

    /// Run one tick and, if it changed persisted schedule state, write the
    /// snapshot through. `last_fired` updates and one-shot `at(...)` deletions
    /// are otherwise only in memory and lost on restart. Only persists on a
    /// mutating tick, to avoid a snapshot every idle second.
    async fn tick_and_persist(
        &self,
        cron_last_minute: &mut HashMap<(String, ScheduleKey), CronFireStamp>,
        pending_fires: &mut HashMap<(String, ScheduleKey), PendingFire>,
        retries: &mut HashMap<(String, ScheduleKey), RetryState>,
    ) {
        let mutated = self.tick(cron_last_minute, pending_fires, retries);
        if mutated {
            save_scheduler_snapshot(
                &self.state,
                self.snapshot_store.clone(),
                &self.snapshot_lock,
            )
            .await;
        }
    }

    /// Run one firing pass. Returns `true` if it mutated persisted schedule
    /// state (set `last_fired` on a fired schedule or deleted a completed
    /// one-shot), so the caller knows to write the snapshot through.
    fn tick(
        &self,
        cron_last_minute: &mut HashMap<(String, ScheduleKey), CronFireStamp>,
        pending_fires: &mut HashMap<(String, ScheduleKey), PendingFire>,
        retries: &mut HashMap<(String, ScheduleKey), RetryState>,
    ) -> bool {
        let now = Utc::now();
        // Phase 1: collect due schedules while holding a short write lock.
        let mut due: Vec<(String, ScheduleKey)> = Vec::new();
        let mut post_fire_actions: Vec<(String, ScheduleKey, PostFire)> = Vec::new();
        {
            let mut accounts = self.state.write();
            let account_ids: Vec<String> = accounts.iter().map(|(id, _)| id.to_string()).collect();
            for account_id in account_ids {
                let Some(state) = accounts.get_mut(&account_id) else {
                    continue;
                };
                let keys: Vec<ScheduleKey> = state.schedules.keys().cloned().collect();
                for key in keys {
                    let Some(sched) = state.schedules.get(&key) else {
                        continue;
                    };
                    if sched.state != "ENABLED" {
                        continue;
                    }
                    if let Some(end) = sched.end_date {
                        if now > end {
                            continue;
                        }
                    }
                    if let Some(start) = sched.start_date {
                        if now < start {
                            continue;
                        }
                    }
                    let Some(expr) = expr::parse(&sched.schedule_expression) else {
                        continue;
                    };
                    let tz = sched.schedule_expression_timezone.clone();
                    let pending_key = (account_id.clone(), key.clone());

                    // 1. Check pending FIRST so retries and FLEXIBLE
                    //    deferrals fire even when the underlying
                    //    expression isn't "due" again.
                    if let Some(p) = pending_fires.get(&pending_key).copied() {
                        if now >= p.fire_at {
                            pending_fires.remove(&pending_key);
                            if let Some(sched_mut) = state.schedules.get_mut(&key) {
                                sched_mut.last_fired = Some(now);
                                let post = post_fire_action(&expr, sched_mut);
                                post_fire_actions.push((account_id.clone(), key.clone(), post));
                            }
                            due.push(pending_key);
                            continue;
                        }
                        // Pending exists but not yet ready — don't
                        // re-evaluate is_due for this schedule, the
                        // queued fire owns it.
                        continue;
                    }

                    if !is_due_with_dedup(
                        &expr,
                        sched.last_fired,
                        now,
                        tz.as_deref(),
                        &pending_key,
                        cron_last_minute,
                    ) {
                        continue;
                    }

                    // FlexibleTimeWindow: when MODE=FLEXIBLE, defer the
                    // first fire by a deterministic random offset within
                    // [0, max_window_in_minutes*60) seconds. The offset
                    // is picked the first tick the schedule is due and
                    // honored on subsequent ticks until reached.
                    let window_minutes = match sched.flexible_time_window.mode.as_str() {
                        "FLEXIBLE" => sched
                            .flexible_time_window
                            .maximum_window_in_minutes
                            .unwrap_or(0)
                            .max(0),
                        _ => 0,
                    };
                    if window_minutes > 0 {
                        let offset_seconds = stable_offset_seconds(&sched.arn, now, window_minutes);
                        let fire_at = now + chrono::Duration::seconds(offset_seconds);
                        pending_fires.insert(pending_key, PendingFire { fire_at });
                        continue;
                    }

                    if let Some(sched_mut) = state.schedules.get_mut(&key) {
                        sched_mut.last_fired = Some(now);
                        let post = post_fire_action(&expr, sched_mut);
                        post_fire_actions.push((account_id.clone(), key.clone(), post));
                    }
                    due.push((account_id.clone(), key));
                }
            }
        }

        // Phase 2: deliver without holding the state lock.
        for (account_id, key) in &due {
            let snapshot = {
                let accounts = self.state.read();
                accounts
                    .get(account_id)
                    .and_then(|s| s.schedules.get(key).cloned())
            };
            let Some(sched) = snapshot else {
                continue;
            };
            let retry_key = (account_id.clone(), key.clone());
            match deliver_target(&self.delivery, &sched) {
                Ok(()) => {
                    retries.remove(&retry_key);
                    tracing::debug!(
                        schedule = %sched.name,
                        group = %sched.group_name,
                        target = %sched.target.arn,
                        "scheduler: fired"
                    );
                }
                Err(err) => {
                    let max_attempts = sched
                        .target
                        .retry_policy
                        .as_ref()
                        .and_then(|r| r.maximum_retry_attempts)
                        .unwrap_or(0);
                    let max_age = sched
                        .target
                        .retry_policy
                        .as_ref()
                        .and_then(|r| r.maximum_event_age_in_seconds);
                    let entry = retries.entry(retry_key.clone()).or_insert(RetryState {
                        attempts: 0,
                        first_due: now,
                        last_error: err.to_string(),
                    });
                    entry.attempts += 1;
                    entry.last_error = err.to_string();
                    let aged_out = max_age
                        .map(|s| (now - entry.first_due).num_seconds() >= s)
                        .unwrap_or(false);
                    if entry.attempts <= max_attempts && !aged_out {
                        // Exponential backoff capped at 60s: 1s, 2s, 4s,
                        // 8s, ... AWS Scheduler does not document the
                        // exact curve, but using a bounded geometric
                        // schedule ensures retry budget exhaustion and
                        // MaximumEventAgeInSeconds enforcement remain
                        // observable in tests.
                        let backoff = backoff_seconds(entry.attempts);
                        pending_fires.insert(
                            retry_key,
                            PendingFire {
                                fire_at: now + chrono::Duration::seconds(backoff),
                            },
                        );
                    } else {
                        let last_error = entry.last_error.clone();
                        retries.remove(&retry_key);
                        route_to_dlq(&self.delivery, &sched, "TargetDeliveryFailed", &last_error);
                    }
                }
            }
        }

        // Phase 3: apply post-fire actions (DELETE on completion for at()).
        if !post_fire_actions.is_empty() {
            let mut accounts = self.state.write();
            for (account_id, key, post) in post_fire_actions {
                let Some(state) = accounts.get_mut(&account_id) else {
                    continue;
                };
                match post {
                    PostFire::Delete => {
                        state.schedules.remove(&key);
                    }
                    PostFire::None => {}
                }
            }
        }

        // Any schedule that fired had its `last_fired` advanced (and a
        // completed one-shot was deleted), so persisted state changed.
        !due.is_empty()
    }
}

/// Geometric backoff schedule for failed target deliveries. Returns
/// `1s, 2s, 4s, 8s, ..., 60s` then caps. Caller passes the current
/// attempt count (1-indexed: first retry is `attempt=1`).
fn backoff_seconds(attempt: i64) -> i64 {
    const CAP_SECONDS: i64 = 60;
    let shift = attempt.saturating_sub(1).clamp(0, 30) as u32;
    (1_i64 << shift).min(CAP_SECONDS)
}

/// Pick a uniform-random offset within `[0, window_minutes*60]` seconds
/// for a given schedule. Reproducible: the RNG is seeded from a hash of
/// `(schedule_arn, fire_at_unix_minute)` so the same `(schedule, minute)`
/// pair yields the same offset across ticks (the offset is honored on
/// every tick until it lands), and tests can predict the value without
/// freezing wall time.
fn stable_offset_seconds(schedule_arn: &str, now: DateTime<Utc>, window_minutes: i64) -> i64 {
    use rand::{Rng, SeedableRng};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    schedule_arn.hash(&mut h);
    now.timestamp().div_euclid(60).hash(&mut h);
    let seed = h.finish();
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let upper = (window_minutes * 60).max(0) as u64;
    // Inclusive upper: AWS's FlexibleTimeWindow spec covers the whole
    // [0, MaximumWindowInMinutes*60] window.
    rng.gen_range(0..=upper) as i64
}

enum PostFire {
    None,
    Delete,
}

fn post_fire_action(expr: &Expr, schedule: &crate::state::Schedule) -> PostFire {
    if matches!(expr, Expr::At(_)) && schedule.action_after_completion == "DELETE" {
        PostFire::Delete
    } else {
        PostFire::None
    }
}

/// Identity of the last minute-slot a cron schedule fired in. Keyed
/// by full (year, ordinal, hour, minute) so subsequent days of the
/// same (hour, minute) are treated as fresh fires. The fields come
/// from the schedule's local timezone (or UTC when no tz is set) so
/// a cron firing at local midnight dedupes across the whole 24-hour
/// window even when UTC has already rolled over.
#[derive(Clone, Copy, PartialEq, Eq)]
struct CronFireStamp {
    year: i32,
    ordinal: u32,
    hour: u32,
    minute: u32,
}

impl CronFireStamp {
    fn from_utc(now: DateTime<Utc>) -> Self {
        Self {
            year: now.year(),
            ordinal: now.ordinal(),
            hour: now.hour(),
            minute: now.minute(),
        }
    }

    fn from_local(now: DateTime<Utc>, tz: &str) -> Self {
        match tz.parse::<chrono_tz::Tz>() {
            Ok(tz) => {
                let local = now.with_timezone(&tz);
                Self {
                    year: local.year(),
                    ordinal: local.ordinal(),
                    hour: local.hour(),
                    minute: local.minute(),
                }
            }
            Err(_) => Self::from_utc(now),
        }
    }
}

fn is_due_with_dedup(
    expr: &Expr,
    last_fired: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    tz: Option<&str>,
    key: &(String, ScheduleKey),
    cron_last_minute: &mut HashMap<(String, ScheduleKey), CronFireStamp>,
) -> bool {
    match expr {
        Expr::Cron(c) => {
            let matched = match tz {
                Some(tz) => expr::matches_cron_in_tz(c, now, tz),
                None => expr::matches_cron(c, now),
            };
            if !matched {
                return false;
            }
            let current = match tz {
                Some(tz) => CronFireStamp::from_local(now, tz),
                None => CronFireStamp::from_utc(now),
            };
            if cron_last_minute.get(key) == Some(&current) {
                return false;
            }
            cron_last_minute.insert(key.clone(), current);
            true
        }
        _ => expr::is_due(expr, last_fired, now),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{FlexibleTimeWindow, Schedule, SharedSchedulerState, Target};
    use chrono::{TimeZone, Utc};
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::delivery::{SqsDelivery, SqsDeliveryError, SqsMessageAttribute};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    const ACCOUNT: &str = "111122223333";

    fn make_state() -> SharedSchedulerState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "us-east-1", ""),
        ))
    }

    fn seed_schedule(state: &SharedSchedulerState, name: &str, expr: &str, target_arn: &str) {
        let now = Utc::now();
        let mut accounts = state.write();
        let s = accounts.get_or_create(ACCOUNT);
        s.schedules.insert(
            ("default".to_string(), name.to_string()),
            Schedule {
                arn: Arn::new(
                    "scheduler",
                    "us-east-1",
                    ACCOUNT,
                    &format!("schedule/default/{name}"),
                )
                .to_string(),
                name: name.to_string(),
                group_name: "default".to_string(),
                schedule_expression: expr.to_string(),
                schedule_expression_timezone: None,
                start_date: None,
                end_date: None,
                description: None,
                state: "ENABLED".to_string(),
                kms_key_arn: None,
                action_after_completion: "NONE".to_string(),
                flexible_time_window: FlexibleTimeWindow::default(),
                target: Target {
                    arn: target_arn.to_string(),
                    role_arn: "arn:aws:iam::1:role/s".to_string(),
                    input: Some(r#"{"msg":"hello"}"#.to_string()),
                    dead_letter_config: None,
                    retry_policy: None,
                    sqs_parameters: None,
                    ecs_parameters: None,
                    eventbridge_parameters: None,
                    kinesis_parameters: None,
                    sagemaker_pipeline_parameters: None,
                },
                creation_date: now,
                last_modification_date: now,
                last_fired: None,
            },
        );
    }

    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<(String, String)>>,
    }
    impl SqsDelivery for Recorder {
        fn deliver_to_queue(&self, _arn: &str, _body: &str, _a: &HashMap<String, String>) {}

        fn try_deliver_to_queue_with_attrs(
            &self,
            queue_arn: &str,
            message_body: &str,
            _attrs: &HashMap<String, SqsMessageAttribute>,
            _g: Option<&str>,
            _d: Option<&str>,
        ) -> Result<(), SqsDeliveryError> {
            self.calls
                .lock()
                .unwrap()
                .push((queue_arn.to_string(), message_body.to_string()));
            Ok(())
        }
    }

    #[test]
    fn rate_schedule_fires_on_first_tick_and_delivers_input() {
        let state = make_state();
        seed_schedule(
            &state,
            "r",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:q",
        );
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();
        ticker.tick(&mut cron, &mut pending, &mut retries);
        let calls = rec.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "arn:aws:sqs:us-east-1:111122223333:q");
        assert_eq!(calls[0].1, r#"{"msg":"hello"}"#);
    }

    #[test]
    fn disabled_schedule_does_not_fire() {
        let state = make_state();
        seed_schedule(
            &state,
            "r",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:q",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            s.schedules
                .get_mut(&("default".to_string(), "r".to_string()))
                .unwrap()
                .state = "DISABLED".to_string();
        }
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();
        ticker.tick(&mut cron, &mut pending, &mut retries);
        assert!(rec.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn at_one_shot_with_delete_after_fire_removes_schedule() {
        let state = make_state();
        // `at(2000-01-01T00:00:00)` is always in the past so it fires on first tick.
        seed_schedule(
            &state,
            "once",
            "at(2000-01-01T00:00:00)",
            "arn:aws:sqs:us-east-1:111122223333:dest",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            s.schedules
                .get_mut(&("default".to_string(), "once".to_string()))
                .unwrap()
                .action_after_completion = "DELETE".to_string();
        }
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();
        ticker.tick(&mut cron, &mut pending, &mut retries);
        assert_eq!(rec.calls.lock().unwrap().len(), 1);
        let accounts = state.read();
        let s = accounts.get(ACCOUNT).unwrap();
        assert!(!s
            .schedules
            .contains_key(&("default".to_string(), "once".to_string())));
    }

    #[test]
    fn dlq_receives_failed_delivery_with_headers() {
        let state = make_state();
        seed_schedule(
            &state,
            "dlqtest",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:missing",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            s.schedules
                .get_mut(&("default".to_string(), "dlqtest".to_string()))
                .unwrap()
                .target
                .dead_letter_config = Some(crate::state::DeadLetterConfig {
                arn: Some("arn:aws:sqs:us-east-1:111122223333:dlq".to_string()),
            });
        }

        // Recorder that fails on missing queue and records everything else.
        struct FailingRecorder {
            calls: Mutex<Vec<(String, HashMap<String, SqsMessageAttribute>)>>,
        }
        impl SqsDelivery for FailingRecorder {
            fn deliver_to_queue(&self, _a: &str, _b: &str, _c: &HashMap<String, String>) {}
            fn try_deliver_to_queue_with_attrs(
                &self,
                queue_arn: &str,
                _body: &str,
                attrs: &HashMap<String, SqsMessageAttribute>,
                _g: Option<&str>,
                _d: Option<&str>,
            ) -> Result<(), SqsDeliveryError> {
                if queue_arn.ends_with(":missing") {
                    return Err(SqsDeliveryError::QueueNotFound(queue_arn.to_string()));
                }
                self.calls
                    .lock()
                    .unwrap()
                    .push((queue_arn.to_string(), attrs.clone()));
                Ok(())
            }
        }

        let rec = Arc::new(FailingRecorder {
            calls: Mutex::new(Vec::new()),
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();
        ticker.tick(&mut cron, &mut pending, &mut retries);
        let calls = rec.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].0.ends_with(":dlq"));
        assert!(calls[0].1.contains_key("X-Amz-Scheduler-Attempt"));
    }

    #[test]
    fn retry_policy_retries_before_routing_to_dlq() {
        // Schedule with MaximumRetryAttempts=2 should attempt delivery
        // 3 times (initial + 2 retries) before falling through to DLQ.
        let state = make_state();
        seed_schedule(
            &state,
            "retried",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:missing",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            let sched = s
                .schedules
                .get_mut(&("default".to_string(), "retried".to_string()))
                .unwrap();
            sched.target.retry_policy = Some(crate::state::RetryPolicy {
                maximum_event_age_in_seconds: Some(86400),
                maximum_retry_attempts: Some(2),
            });
            sched.target.dead_letter_config = Some(crate::state::DeadLetterConfig {
                arn: Some("arn:aws:sqs:us-east-1:111122223333:dlq".to_string()),
            });
        }

        // Track which queue each delivery went to.
        struct Failing {
            calls: Mutex<Vec<String>>,
        }
        impl SqsDelivery for Failing {
            fn deliver_to_queue(&self, _: &str, _: &str, _: &HashMap<String, String>) {}
            fn try_deliver_to_queue_with_attrs(
                &self,
                queue_arn: &str,
                _body: &str,
                _attrs: &HashMap<String, SqsMessageAttribute>,
                _g: Option<&str>,
                _d: Option<&str>,
            ) -> Result<(), SqsDeliveryError> {
                self.calls.lock().unwrap().push(queue_arn.to_string());
                if queue_arn.ends_with(":missing") {
                    Err(SqsDeliveryError::QueueNotFound(queue_arn.to_string()))
                } else {
                    Ok(())
                }
            }
        }
        let rec = Arc::new(Failing {
            calls: Mutex::new(Vec::new()),
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();

        // Tick 1: initial fire fails, retry queued.
        ticker.tick(&mut cron, &mut pending, &mut retries);
        // Manually advance the pending fire so the next tick can pick it up.
        for p in pending.values_mut() {
            p.fire_at = Utc::now() - chrono::Duration::seconds(1);
        }
        // Tick 2: first retry, fails, second retry queued.
        ticker.tick(&mut cron, &mut pending, &mut retries);
        for p in pending.values_mut() {
            p.fire_at = Utc::now() - chrono::Duration::seconds(1);
        }
        // Tick 3: second retry, fails, retry budget exhausted -> DLQ.
        ticker.tick(&mut cron, &mut pending, &mut retries);

        let calls = rec.calls.lock().unwrap();
        // 3 attempts at the missing queue + 1 DLQ delivery.
        let missing_attempts = calls.iter().filter(|q| q.ends_with(":missing")).count();
        let dlq_deliveries = calls.iter().filter(|q| q.ends_with(":dlq")).count();
        assert_eq!(
            missing_attempts, 3,
            "expected initial + 2 retries before exhausting RetryPolicy budget"
        );
        assert_eq!(
            dlq_deliveries, 1,
            "DLQ must fire exactly once after exhaust"
        );
    }

    #[test]
    fn flexible_time_window_defers_fire_within_window() {
        // FLEXIBLE mode + max_window_in_minutes=2 means the schedule
        // must NOT fire on the first tick when due — it gets queued
        // for a deferred fire up to 120s out.
        let state = make_state();
        seed_schedule(
            &state,
            "flex",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:q",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            let sched = s
                .schedules
                .get_mut(&("default".to_string(), "flex".to_string()))
                .unwrap();
            sched.flexible_time_window = FlexibleTimeWindow {
                mode: "FLEXIBLE".to_string(),
                maximum_window_in_minutes: Some(2),
            };
        }
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();

        ticker.tick(&mut cron, &mut pending, &mut retries);
        assert!(
            rec.calls.lock().unwrap().is_empty(),
            "FLEXIBLE-windowed schedule must defer first fire, not deliver immediately"
        );
        assert_eq!(
            pending.len(),
            1,
            "deferred fire must be queued for a later tick"
        );

        // Advance the pending timestamp into the past and tick again.
        for p in pending.values_mut() {
            p.fire_at = Utc::now() - chrono::Duration::seconds(1);
        }
        ticker.tick(&mut cron, &mut pending, &mut retries);
        assert_eq!(
            rec.calls.lock().unwrap().len(),
            1,
            "deferred fire must deliver once its window timestamp is reached"
        );
    }

    #[test]
    fn end_date_past_prevents_firing() {
        let state = make_state();
        seed_schedule(
            &state,
            "ended",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:q",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            s.schedules
                .get_mut(&("default".to_string(), "ended".to_string()))
                .unwrap()
                .end_date = Some(Utc::now() - chrono::Duration::hours(1));
        }
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();
        ticker.tick(&mut cron, &mut pending, &mut retries);
        assert!(rec.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn backoff_seconds_is_exponential_and_capped() {
        // 1, 2, 4, 8, 16, 32, 60 (cap), 60, ...
        assert_eq!(super::backoff_seconds(1), 1);
        assert_eq!(super::backoff_seconds(2), 2);
        assert_eq!(super::backoff_seconds(3), 4);
        assert_eq!(super::backoff_seconds(4), 8);
        assert_eq!(super::backoff_seconds(5), 16);
        assert_eq!(super::backoff_seconds(6), 32);
        assert_eq!(super::backoff_seconds(7), 60);
        assert_eq!(super::backoff_seconds(20), 60);
        // Defensive: 0 and negatives shouldn't panic.
        assert_eq!(super::backoff_seconds(0), 1);
        assert_eq!(super::backoff_seconds(-1), 1);
    }

    #[test]
    fn stable_offset_seconds_is_reproducible_and_within_bounds() {
        let now = Utc.with_ymd_and_hms(2026, 5, 1, 12, 30, 15).unwrap();
        let arn = "arn:aws:scheduler:us-east-1:1:schedule/default/foo";
        // Same (arn, minute) -> same offset.
        let a = super::stable_offset_seconds(arn, now, 5);
        let b = super::stable_offset_seconds(arn, now, 5);
        assert_eq!(a, b, "offset must be stable per (arn, minute)");
        // Within [0, 5*60].
        assert!((0..=300).contains(&a), "offset {a} not in [0, 300]");
        // Different schedule ARN -> almost certainly different offset.
        let c = super::stable_offset_seconds(
            "arn:aws:scheduler:us-east-1:1:schedule/default/bar",
            now,
            5,
        );
        assert!((0..=300).contains(&c));
    }

    #[test]
    fn retry_then_success_clears_retry_state_and_delivers_to_target() {
        // Schedule with MaxRetryAttempts=3, target succeeds on the 3rd attempt.
        let state = make_state();
        seed_schedule(
            &state,
            "retry-then-ok",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:flaky",
        );
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(ACCOUNT);
            let sched = s
                .schedules
                .get_mut(&("default".to_string(), "retry-then-ok".to_string()))
                .unwrap();
            sched.target.retry_policy = Some(crate::state::RetryPolicy {
                maximum_event_age_in_seconds: Some(86400),
                maximum_retry_attempts: Some(3),
            });
        }

        struct FlakyN {
            calls: Mutex<Vec<String>>,
            fail_first_n: i64,
        }
        impl SqsDelivery for FlakyN {
            fn deliver_to_queue(&self, _: &str, _: &str, _: &HashMap<String, String>) {}
            fn try_deliver_to_queue_with_attrs(
                &self,
                queue_arn: &str,
                _body: &str,
                _attrs: &HashMap<String, SqsMessageAttribute>,
                _g: Option<&str>,
                _d: Option<&str>,
            ) -> Result<(), SqsDeliveryError> {
                let mut calls = self.calls.lock().unwrap();
                calls.push(queue_arn.to_string());
                if (calls.len() as i64) <= self.fail_first_n {
                    Err(SqsDeliveryError::QueueNotFound(queue_arn.to_string()))
                } else {
                    Ok(())
                }
            }
        }
        let rec = Arc::new(FlakyN {
            calls: Mutex::new(Vec::new()),
            fail_first_n: 2,
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let ticker = Ticker::new(state.clone(), bus);
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();

        // Tick 1: initial fire fails (call #1), retry queued.
        ticker.tick(&mut cron, &mut pending, &mut retries);
        assert_eq!(retries.len(), 1, "retry state must be tracked");
        for p in pending.values_mut() {
            p.fire_at = Utc::now() - chrono::Duration::seconds(1);
        }
        // Tick 2: first retry fails (call #2), retry queued.
        ticker.tick(&mut cron, &mut pending, &mut retries);
        for p in pending.values_mut() {
            p.fire_at = Utc::now() - chrono::Duration::seconds(1);
        }
        // Tick 3: second retry succeeds (call #3) -> retry state cleared.
        ticker.tick(&mut cron, &mut pending, &mut retries);

        let calls = rec.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            3,
            "expected 3 delivery attempts (2 fail + 1 ok)"
        );
        assert!(
            retries.is_empty(),
            "retry state must be cleared after success"
        );
    }

    /// A SnapshotStore that records the last bytes saved (the real
    /// MemorySnapshotStore is a no-op), so a test can assert the ticker wrote
    /// through.
    #[derive(Default)]
    struct RecordingSnap {
        bytes: std::sync::Mutex<Option<Vec<u8>>>,
    }
    impl fakecloud_persistence::SnapshotStore for RecordingSnap {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.bytes.lock().unwrap().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.bytes.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
    }

    #[tokio::test]
    async fn fired_schedule_is_written_through() {
        // A fired schedule advances last_fired in memory; without write-through
        // it (and a self-deleted one-shot) is lost on restart (0.A5). A tick
        // that fires must persist; an idle tick must not.
        let state = make_state();
        seed_schedule(
            &state,
            "r",
            "rate(1 minute)",
            "arn:aws:sqs:us-east-1:111122223333:q",
        );
        let store = Arc::new(RecordingSnap::default());
        let ticker = Ticker::new(state.clone(), Arc::new(DeliveryBus::new()))
            .with_snapshot_store(store.clone());
        let mut cron = HashMap::new();
        let mut pending = HashMap::new();
        let mut retries = HashMap::new();

        // First tick: rate schedule is due -> fires -> writes through.
        ticker
            .tick_and_persist(&mut cron, &mut pending, &mut retries)
            .await;
        assert!(
            fakecloud_persistence::SnapshotStore::load(store.as_ref())
                .unwrap()
                .is_some(),
            "a fired schedule must be written through to the snapshot"
        );

        // Reset the recorder; a second tick in the same minute isn't due again,
        // so it must NOT snapshot (avoid a write every idle second).
        *store.bytes.lock().unwrap() = None;
        ticker
            .tick_and_persist(&mut cron, &mut pending, &mut retries)
            .await;
        assert!(
            fakecloud_persistence::SnapshotStore::load(store.as_ref())
                .unwrap()
                .is_none(),
            "an idle tick must not snapshot"
        );
    }
}
