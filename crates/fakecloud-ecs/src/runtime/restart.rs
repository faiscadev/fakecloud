//! Container restart policies (`restartPolicy`) and the dependency
//! `startTimeout`, shared by the Docker/Podman and Kubernetes backends.
//!
//! The decisions mirror the ECS container agent:
//! - A container with `restartPolicy.enabled` is restarted in place (same
//!   task) whenever it exits, whatever the exit code (0 included), unless the
//!   code is in `ignoredExitCodes`, the task is being stopped, or it has not
//!   run for `restartAttemptPeriod` seconds since it last (re)started. A
//!   container that is not restarted exits as it would without a policy: an
//!   essential one stops the task.
//! - A dependent container waiting on a `COMPLETE`/`SUCCESS`/`HEALTHY`
//!   condition gives up once the upstream has been started for longer than
//!   the upstream's own `startTimeout`. `START` never times out, and an
//!   upstream without a `startTimeout` is waited on until it resolves.

use std::time::{Duration, Instant};

use super::DependsOnCondition;

/// `restartAttemptPeriod` when the policy doesn't set one.
pub(crate) const DEFAULT_RESTART_ATTEMPT_PERIOD_SECS: u64 = 300;
/// Smallest `restartAttemptPeriod` ECS accepts.
pub(crate) const MIN_RESTART_ATTEMPT_PERIOD_SECS: i64 = 60;
/// Largest `restartAttemptPeriod` ECS accepts.
pub(crate) const MAX_RESTART_ATTEMPT_PERIOD_SECS: i64 = 1800;
/// Most `ignoredExitCodes` one policy may list.
pub(crate) const MAX_IGNORED_EXIT_CODES: usize = 50;
/// `stopTimeout` when the container definition doesn't set one: the grace
/// between SIGTERM and SIGKILL when ECS stops a container.
pub(crate) const DEFAULT_STOP_TIMEOUT_SECS: u32 = 30;

/// An enabled `restartPolicy`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RestartPolicy {
    pub ignored_exit_codes: Vec<i64>,
    pub attempt_period: Duration,
}

/// The enabled restart policy of a container definition, `None` when it has
/// none or it is disabled.
pub(crate) fn parse_restart_policy(
    container_definition: &serde_json::Value,
) -> Option<RestartPolicy> {
    let policy = container_definition.get("restartPolicy")?;
    if policy.get("enabled").and_then(|v| v.as_bool()) != Some(true) {
        return None;
    }
    let ignored_exit_codes = policy
        .get("ignoredExitCodes")
        .and_then(|v| v.as_array())
        .map(|codes| codes.iter().filter_map(|c| c.as_i64()).collect())
        .unwrap_or_default();
    let attempt_period = policy
        .get("restartAttemptPeriod")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_RESTART_ATTEMPT_PERIOD_SECS);
    Some(RestartPolicy {
        ignored_exit_codes,
        attempt_period: Duration::from_secs(attempt_period),
    })
}

/// Register-time validation of a container definition's `restartPolicy`.
/// `Err` carries the `ClientException` message.
pub(crate) fn validate_restart_policy(
    container_definition: &serde_json::Value,
) -> Result<(), String> {
    let Some(policy) = container_definition.get("restartPolicy") else {
        return Ok(());
    };
    if policy.is_null() {
        return Ok(());
    }
    if !policy.is_object() {
        return Err("restartPolicy must be an object".to_string());
    }
    if !policy.get("enabled").is_some_and(|v| v.is_boolean()) {
        return Err("restartPolicy.enabled is required and must be a boolean".to_string());
    }
    if let Some(codes) = policy.get("ignoredExitCodes").filter(|v| !v.is_null()) {
        let Some(codes) = codes.as_array() else {
            return Err("restartPolicy.ignoredExitCodes must be a list of integers".to_string());
        };
        if codes.iter().any(|c| c.as_i64().is_none()) {
            return Err("restartPolicy.ignoredExitCodes must be a list of integers".to_string());
        }
        if codes.len() > MAX_IGNORED_EXIT_CODES {
            return Err(format!(
                "restartPolicy.ignoredExitCodes can contain at most {MAX_IGNORED_EXIT_CODES} exit codes"
            ));
        }
    }
    if let Some(period) = policy.get("restartAttemptPeriod").filter(|v| !v.is_null()) {
        let in_range = period.as_i64().is_some_and(|p| {
            (MIN_RESTART_ATTEMPT_PERIOD_SECS..=MAX_RESTART_ATTEMPT_PERIOD_SECS).contains(&p)
        });
        if !in_range {
            return Err(format!(
                "restartPolicy.restartAttemptPeriod must be between \
                 {MIN_RESTART_ATTEMPT_PERIOD_SECS} and {MAX_RESTART_ATTEMPT_PERIOD_SECS} seconds"
            ));
        }
    }
    Ok(())
}

/// Why an exited container is not restarted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoRestart {
    /// The task is being stopped.
    TaskStopping,
    /// The exit code is unknown.
    UnknownExitCode,
    /// The exit code is in `ignoredExitCodes`.
    IgnoredExitCode,
    /// The container exited before running for `restartAttemptPeriod`.
    AttemptPeriodNotElapsed,
}

/// Whether a container that exited with `exit_code` after running for
/// `ran_for` since its last (re)start is restarted under `policy`.
pub(crate) fn restart_decision(
    policy: &RestartPolicy,
    exit_code: Option<i64>,
    ran_for: Duration,
    task_stopping: bool,
) -> Result<(), NoRestart> {
    if task_stopping {
        return Err(NoRestart::TaskStopping);
    }
    let Some(code) = exit_code else {
        return Err(NoRestart::UnknownExitCode);
    };
    if policy.ignored_exit_codes.contains(&code) {
        return Err(NoRestart::IgnoredExitCode);
    }
    if ran_for < policy.attempt_period {
        return Err(NoRestart::AttemptPeriodNotElapsed);
    }
    Ok(())
}

/// Restart bookkeeping for one running container with a restart policy.
#[derive(Clone, Debug)]
pub(crate) struct RestartTracker {
    pub policy: RestartPolicy,
    /// Restarts so far (`RestartCount` in the task metadata endpoint).
    pub restart_count: u32,
    /// When the container last (re)started.
    pub last_started: Instant,
}

impl RestartTracker {
    pub(crate) fn new(policy: RestartPolicy, started: Instant) -> Self {
        Self {
            policy,
            restart_count: 0,
            last_started: started,
        }
    }

    /// Whether the container, having exited with `exit_code` at `now`, is
    /// restarted.
    pub(crate) fn decide(
        &self,
        exit_code: Option<i64>,
        now: Instant,
        task_stopping: bool,
    ) -> Result<(), NoRestart> {
        restart_decision(
            &self.policy,
            exit_code,
            now.saturating_duration_since(self.last_started),
            task_stopping,
        )
    }

    /// Record a restart performed at `at`.
    pub(crate) fn record_restart(&mut self, at: Instant) {
        self.restart_count += 1;
        self.last_started = at;
    }
}

/// Whether a dependent waiting on `condition` gives up on an upstream that
/// started at `upstream_started` and has `upstream_start_timeout` seconds.
pub(crate) fn dependency_timed_out(
    condition: DependsOnCondition,
    upstream_start_timeout: Option<u32>,
    upstream_started: Instant,
    now: Instant,
) -> bool {
    if matches!(condition, DependsOnCondition::Start) {
        return false;
    }
    match upstream_start_timeout {
        Some(secs) if secs > 0 => {
            now.saturating_duration_since(upstream_started) >= Duration::from_secs(secs.into())
        }
        _ => false,
    }
}

/// The task's `stoppedReason` when a dependency times out.
pub(crate) fn dependency_timeout_reason(
    dependent: &str,
    upstream: &str,
    condition: DependsOnCondition,
    start_timeout: u32,
) -> String {
    format!(
        "container {dependent} did not start: dependency on container {upstream} did not reach \
         {} within its startTimeout of {start_timeout}s",
        condition.as_aws_str()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy(ignored: &[i64], period: u64) -> RestartPolicy {
        RestartPolicy {
            ignored_exit_codes: ignored.to_vec(),
            attempt_period: Duration::from_secs(period),
        }
    }

    #[test]
    fn parse_reads_enabled_policy_with_defaults() {
        let p = parse_restart_policy(&json!({"restartPolicy": {"enabled": true}})).unwrap();
        assert!(p.ignored_exit_codes.is_empty());
        assert_eq!(p.attempt_period, Duration::from_secs(300));
        let p = parse_restart_policy(&json!({"restartPolicy": {
            "enabled": true, "ignoredExitCodes": [0, 2], "restartAttemptPeriod": 60
        }}))
        .unwrap();
        assert_eq!(p.ignored_exit_codes, vec![0, 2]);
        assert_eq!(p.attempt_period, Duration::from_secs(60));
    }

    #[test]
    fn parse_ignores_disabled_or_missing_policy() {
        assert!(parse_restart_policy(&json!({})).is_none());
        assert!(parse_restart_policy(&json!({"restartPolicy": {"enabled": false}})).is_none());
    }

    #[test]
    fn validate_enforces_model_limits() {
        let ok = |v| validate_restart_policy(&json!({ "restartPolicy": v }));
        assert!(ok(json!({"enabled": true})).is_ok());
        assert!(ok(json!({"enabled": false, "restartAttemptPeriod": 1800})).is_ok());
        assert!(ok(json!({"enabled": true, "restartAttemptPeriod": 60})).is_ok());
        assert!(ok(json!({"enabled": true, "restartAttemptPeriod": 59})).is_err());
        assert!(ok(json!({"enabled": true, "restartAttemptPeriod": 1801})).is_err());
        assert!(ok(json!({"restartAttemptPeriod": 300})).is_err());
        let fifty: Vec<i64> = (0..50).collect();
        let fifty_one: Vec<i64> = (0..51).collect();
        assert!(ok(json!({"enabled": true, "ignoredExitCodes": fifty})).is_ok());
        assert!(ok(json!({"enabled": true, "ignoredExitCodes": fifty_one})).is_err());
        assert!(ok(json!({"enabled": true, "ignoredExitCodes": ["1"]})).is_err());
        assert!(validate_restart_policy(&json!({})).is_ok());
    }

    #[test]
    fn restarts_any_exit_code_not_ignored() {
        let p = policy(&[0], 60);
        let long = Duration::from_secs(61);
        assert_eq!(restart_decision(&p, Some(1), long, false), Ok(()));
        assert_eq!(restart_decision(&p, Some(137), long, false), Ok(()));
        assert_eq!(
            restart_decision(&p, Some(0), long, false),
            Err(NoRestart::IgnoredExitCode)
        );
        // Exit 0 is restarted too unless ignored.
        assert_eq!(
            restart_decision(&policy(&[], 60), Some(0), long, false),
            Ok(())
        );
        assert_eq!(
            restart_decision(&p, None, long, false),
            Err(NoRestart::UnknownExitCode)
        );
    }

    #[test]
    fn restart_needs_the_attempt_period_and_a_live_task() {
        let p = policy(&[], 60);
        assert_eq!(
            restart_decision(&p, Some(1), Duration::from_secs(59), false),
            Err(NoRestart::AttemptPeriodNotElapsed)
        );
        assert_eq!(
            restart_decision(&p, Some(1), Duration::from_secs(60), false),
            Ok(())
        );
        assert_eq!(
            restart_decision(&p, Some(1), Duration::from_secs(600), true),
            Err(NoRestart::TaskStopping)
        );
    }

    #[test]
    fn tracker_counts_restarts_and_measures_from_the_last_restart() {
        let t0 = Instant::now();
        let mut t = RestartTracker::new(policy(&[], 60), t0);
        let first_exit = t0 + Duration::from_secs(70);
        assert_eq!(t.decide(Some(1), first_exit, false), Ok(()));
        t.record_restart(first_exit);
        assert_eq!(t.restart_count, 1);
        // Exiting 30s after the restart is too soon, though 100s have passed
        // since the container first started.
        assert_eq!(
            t.decide(Some(1), first_exit + Duration::from_secs(30), false),
            Err(NoRestart::AttemptPeriodNotElapsed)
        );
        let second_exit = first_exit + Duration::from_secs(61);
        assert_eq!(t.decide(Some(1), second_exit, false), Ok(()));
        t.record_restart(second_exit);
        assert_eq!(t.restart_count, 2);
    }

    #[test]
    fn dependency_timeout_uses_the_upstream_start_timeout() {
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(10);
        for cond in [
            DependsOnCondition::Complete,
            DependsOnCondition::Success,
            DependsOnCondition::Healthy,
        ] {
            assert!(dependency_timed_out(cond, Some(10), t0, later));
            assert!(!dependency_timed_out(cond, Some(11), t0, later));
            // No startTimeout: waited on until it resolves.
            assert!(!dependency_timed_out(
                cond,
                None,
                t0,
                t0 + Duration::from_secs(86400)
            ));
            assert!(!dependency_timed_out(cond, Some(0), t0, later));
        }
        assert!(!dependency_timed_out(
            DependsOnCondition::Start,
            Some(1),
            t0,
            later
        ));
    }
}
