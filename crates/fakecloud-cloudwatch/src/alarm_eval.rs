//! Alarm evaluation.
//!
//! CloudWatch alarms are not just stored — they transition state based on the
//! metric data they watch. This module implements two real evaluators:
//!
//! - [`evaluate_metric_alarm`] computes a single-metric alarm's state from the
//!   stored datapoints over its `evaluation_periods` x `period` window,
//!   applying the comparison operator, threshold, statistic,
//!   `datapoints_to_alarm` and `treat_missing_data`.
//! - Composite alarms are evaluated by parsing the `AlarmRule` boolean
//!   expression (`ALARM(x)` / `OK(x)` / `INSUFFICIENT_DATA(x)`, `AND`/`OR`/
//!   `NOT`, parentheses, `TRUE`/`FALSE`) and evaluating it against the current
//!   states of the referenced alarms.
//!
//! [`evaluate_alarms`] recomputes every alarm in a region and records state
//! transitions in the alarm history. It is invoked from `DescribeAlarms` so a
//! `PutMetricData` -> `DescribeAlarms` sequence reflects the alarm firing.
//!
//! Missing-data handling follows `treat_missing_data`: `breaching` /
//! `notBreaching` force ALARM / OK, `ignore` keeps the current state, and the
//! default `missing` transitions an alarm with no datapoints to
//! INSUFFICIENT_DATA. A state forced via `SetAlarmState` is kept sticky (so it
//! can drive composite alarms) until real datapoints arrive, at which point
//! data governs the state.
//!
//! Every evaluation transition also produces the computed outputs AWS returns:
//! `StateReasonData` (the evaluated datapoints for a metric alarm, the
//! triggering children for a composite), `StateTransitionedTimestamp` (only
//! moves when `StateValue` changes), `EvaluationState` (`EVALUATION_ERROR` for
//! a single-metric alarm whose configuration can't be evaluated) and, for a
//! composite alarm with an `ActionsSuppressor`, `ActionsSuppressedBy` /
//! `ActionsSuppressedReason` from the suppressor's state and the wait /
//! extension periods.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};

use crate::alarm_actions::AlarmNotification;
use crate::series_index::SeriesKey;
use crate::service::{collect_metric_buckets, resolve_stat};
use crate::state::{
    ActionsSuppressedBy, AlarmHistoryItem, AlarmState, CloudWatchState, EvaluationState,
    MetricAlarm, MetricDatum, PreviousState,
};

/// Timestamp format AWS uses inside `StateReasonData` and history JSON
/// (`2024-01-01T00:00:00.000+0000`).
pub(crate) fn reason_timestamp(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%dT%H:%M:%S%.3f+0000").to_string()
}

/// The result of evaluating one single-metric alarm.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MetricEvaluation {
    pub state: AlarmState,
    pub reason: String,
    pub reason_data: Option<String>,
    /// Whether the evaluation window held any real datapoints.
    pub had_data: bool,
    pub evaluation_state: Option<EvaluationState>,
}

/// One side (old or new) of a state change, as written to the alarm history.
pub(crate) struct StateSnapshot<'a> {
    pub value: AlarmState,
    pub reason: &'a str,
    pub reason_data: Option<&'a str>,
}

impl StateSnapshot<'_> {
    fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("stateValue".into(), json!(self.value.as_str()));
        m.insert("stateReason".into(), json!(self.reason));
        if let Some(d) = self.reason_data {
            // Embedded as an object like AWS does; reason data given to
            // SetAlarmState that isn't JSON is kept as a string.
            let v = serde_json::from_str(d).unwrap_or_else(|_| json!(d));
            m.insert("stateReasonData".into(), v);
        }
        Value::Object(m)
    }
}

/// A `StateUpdate` history item for a change from `old` to `new` at `at`.
pub(crate) fn state_update_item(
    name: &str,
    alarm_type: &str,
    at: DateTime<Utc>,
    old: StateSnapshot<'_>,
    new: StateSnapshot<'_>,
) -> AlarmHistoryItem {
    let summary = format!(
        "Alarm updated from {} to {}",
        old.value.as_str(),
        new.value.as_str()
    );
    let data = json!({
        "version": "1.0",
        "oldState": old.to_json(),
        "newState": new.to_json(),
    });
    AlarmHistoryItem {
        alarm_name: name.to_string(),
        alarm_type: alarm_type.to_string(),
        timestamp: at,
        history_item_type: "StateUpdate".to_string(),
        history_summary: summary,
        history_data: data.to_string(),
    }
}

/// What one evaluation pass changed.
#[derive(Debug, Default)]
pub(crate) struct Evaluated {
    /// Whether any alarm's stored state changed (so it needs persisting).
    pub changed: bool,
    /// The actions owed for the state changes, to run outside the lock.
    pub notifications: Vec<AlarmNotification>,
    /// How many metric alarms were evaluated.
    pub alarms_evaluated: usize,
    /// How many stored datapoints those evaluations read.
    pub datums_inspected: usize,
}

/// Which alarms an evaluation pass looks at.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Scope<'a> {
    /// Every alarm in the region (DescribeAlarms, a new configuration, a
    /// forced state).
    All,
    /// Alarms whose outcome can have changed with time alone: those past
    /// their next period boundary, and composites with a running
    /// suppression or owed actions (the evaluation timer).
    Due,
    /// Alarms on the series a PutMetricData just wrote to `namespace`.
    Series {
        namespace: &'a str,
        series: &'a [SeriesKey],
    },
}

/// The first period boundary after `now`.
fn next_boundary(now: DateTime<Utc>, period: i64) -> Option<DateTime<Utc>> {
    let secs = now.timestamp();
    DateTime::<Utc>::from_timestamp(secs - secs.rem_euclid(period) + period, 0)
}

/// Evaluate every alarm in `region` against the stored metric data at `now`,
/// updating states, recording transitions in the alarm history and returning
/// the actions they owe.
pub(crate) fn evaluate_alarms(
    state: &mut CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
) -> Evaluated {
    evaluate_alarms_in(state, region, now, Scope::All)
}

/// [`evaluate_alarms`] limited to the alarms `scope` selects.
pub(crate) fn evaluate_alarms_in(
    state: &mut CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
    scope: Scope<'_>,
) -> Evaluated {
    let mut out = Evaluated::default();
    let account_id = state.account_id.clone();
    let mut metric_transition = false;
    // ---- Metric alarms ----
    let mut history: Vec<AlarmHistoryItem> = Vec::new();
    {
        // Disjoint field borrows: `metrics` shared, `series_index` and
        // `alarms` mutable.
        let data_map = state.metrics.get(region);
        let index = &mut state.series_index;
        if let Some(alarms) = state.alarms.get_mut(region) {
            for alarm in alarms.values_mut() {
                let selected = match scope {
                    Scope::All => true,
                    Scope::Due => alarm.next_evaluation_at.is_none_or(|t| t <= now),
                    Scope::Series { namespace, series } => {
                        alarm.namespace.as_deref() == Some(namespace)
                            && alarm.metric_name.as_ref().is_some_and(|m| {
                                series.iter().any(|(n, d)| n == m && *d == alarm.dimensions)
                            })
                    }
                };
                if !selected {
                    continue;
                }
                // A state forced by SetAlarmState holds until its period
                // ends.
                if let Some(until) = alarm.manual_hold_until {
                    if now < until {
                        alarm.next_evaluation_at = Some(until);
                        continue;
                    }
                    alarm.manual_hold_until = None;
                    out.changed = true;
                }
                alarm.next_evaluation_at = next_boundary(now, alarm.period.unwrap_or(60).max(1));
                if crate::alarm_eval::not_single_metric(alarm) {
                    continue;
                }
                out.alarms_evaluated += 1;
                let data: &[MetricDatum] = alarm
                    .namespace
                    .as_ref()
                    .and_then(|ns| data_map.and_then(|m| m.get(ns)))
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let mut inspected = 0;
                let manually_set = alarm.state_manually_set;
                let outcome = evaluate_metric_alarm(
                    alarm,
                    |start, end| {
                        let (Some(ns), Some(name)) = (&alarm.namespace, &alarm.metric_name) else {
                            return Vec::new();
                        };
                        let found =
                            index.window((region, ns), data, name, &alarm.dimensions, start, end);
                        inspected = found.len();
                        found
                    },
                    now,
                    manually_set,
                );
                out.datums_inspected += inspected;
                let Some(ev) = outcome else {
                    continue;
                };
                // Once real datapoints govern the alarm, a prior manual
                // override no longer applies.
                if ev.had_data {
                    alarm.state_manually_set = false;
                }
                let before = (alarm.state_value, alarm.evaluation_state);
                let prev = PreviousState {
                    value: alarm.state_value,
                    reason: alarm.state_reason.clone(),
                    reason_data: alarm.state_reason_data.clone(),
                    timestamp: alarm.state_updated_timestamp,
                };
                apply_metric_evaluation(alarm, ev, now, &mut history);
                if before != (alarm.state_value, alarm.evaluation_state) {
                    out.changed = true;
                }
                if before.0 != alarm.state_value {
                    metric_transition = true;
                }
                out.notifications
                    .extend(crate::alarm_actions::metric_alarm_notification(
                        &account_id,
                        region,
                        alarm,
                        &prev,
                    ));
            }
        }
    }
    for item in history {
        crate::service::push_alarm_history(state, region, item);
    }

    // ---- Composite alarms ----
    // Composites follow their children, so a targeted or timed pass only
    // needs them when a child changed or a suppression is running out.
    let composites_due = match scope {
        Scope::All => true,
        Scope::Series { .. } => metric_transition,
        Scope::Due => {
            metric_transition
                || state.composite_alarms.get(region).is_some_and(|c| {
                    c.values().any(|a| {
                        a.actions_suppressed_by.is_some() || a.pending_action_from.is_some()
                    })
                })
        }
    };
    if composites_due {
        evaluate_composite_alarms(state, region, now, &mut out);
    }
    let epoch = state.epoch;
    for n in &mut out.notifications {
        n.epoch = epoch;
    }
    out
}

/// Apply an evaluation result to `alarm`. `StateUpdatedTimestamp` moves when
/// either `StateValue` or `EvaluationState` changes (with the reason and reason
/// data), `StateTransitionedTimestamp` and the history only when `StateValue`
/// changes.
fn apply_metric_evaluation(
    alarm: &mut MetricAlarm,
    ev: MetricEvaluation,
    now: DateTime<Utc>,
    history: &mut Vec<AlarmHistoryItem>,
) {
    let value_changed = ev.state != alarm.state_value;
    if !value_changed && ev.evaluation_state == alarm.evaluation_state {
        return;
    }
    if value_changed {
        history.push(state_update_item(
            &alarm.alarm_name,
            "MetricAlarm",
            now,
            StateSnapshot {
                value: alarm.state_value,
                reason: &alarm.state_reason,
                reason_data: alarm.state_reason_data.as_deref(),
            },
            StateSnapshot {
                value: ev.state,
                reason: &ev.reason,
                reason_data: ev.reason_data.as_deref(),
            },
        ));
        alarm.state_transitioned_timestamp = Some(now);
        alarm.alarm_exit.note(alarm.state_value, ev.state, now);
    }
    alarm.state_value = ev.state;
    alarm.state_reason = ev.reason;
    alarm.state_reason_data = ev.reason_data;
    alarm.evaluation_state = ev.evaluation_state;
    alarm.state_updated_timestamp = now;
}

/// What a composite rule needs to know about an alarm it can reference.
struct RuleMember {
    arn: String,
    state: AlarmState,
    transitioned: DateTime<Utc>,
}

/// Evaluate the composite alarms in `region`, propagating child states through
/// the rule expressions. Runs to a fixpoint so a composite that references
/// another composite settles. A composite whose state was set by
/// `SetAlarmState` keeps it until one of its children changes state or its
/// configuration is updated, as on AWS.
fn evaluate_composite_alarms(
    state: &mut CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
    out: &mut Evaluated,
) {
    let composite_names: Vec<String> = match state.composite_alarms.get(region) {
        Some(c) if !c.is_empty() => c.keys().cloned().collect(),
        _ => return,
    };
    let account_id = state.account_id.clone();

    // Seed the member map from every alarm a rule can reference.
    let mut members: HashMap<String, RuleMember> = HashMap::new();
    if let Some(a) = state.alarms.get(region) {
        for (n, al) in a.iter() {
            members.insert(
                n.clone(),
                RuleMember {
                    arn: al.alarm_arn.clone(),
                    state: al.state_value,
                    transitioned: al
                        .state_transitioned_timestamp
                        .unwrap_or(al.state_updated_timestamp),
                },
            );
        }
    }
    if let Some(a) = state.log_alarms.get(region) {
        for (n, al) in a.iter() {
            members.insert(
                n.clone(),
                RuleMember {
                    arn: al.alarm_arn.clone(),
                    state: al.state_value,
                    transitioned: al
                        .state_transitioned_timestamp
                        .unwrap_or(al.state_updated_timestamp),
                },
            );
        }
    }
    if let Some(c) = state.composite_alarms.get(region) {
        for (n, al) in c.iter() {
            members.insert(
                n.clone(),
                RuleMember {
                    arn: al.alarm_arn.clone(),
                    state: al.state_value,
                    transitioned: al
                        .state_transitioned_timestamp
                        .unwrap_or(al.state_updated_timestamp),
                },
            );
        }
    }
    let mut states: HashMap<String, AlarmState> =
        members.iter().map(|(n, m)| (n.clone(), m.state)).collect();

    // Manually set composites none of whose children changed since, with the
    // children they reference.
    let mut frozen: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(c) = state.composite_alarms.get_mut(region) {
        for (name, al) in c.iter_mut() {
            if !al.state_manually_set {
                continue;
            }
            let refs: Vec<String> = parse_alarm_rule(&al.alarm_rule)
                .map(|node| {
                    let mut refs = Vec::new();
                    referenced_names(&node, &mut refs);
                    refs.into_iter().map(str::to_string).collect()
                })
                .unwrap_or_default();
            let child_changed = refs.iter().any(|r| {
                members
                    .get(r)
                    .is_some_and(|m| m.transitioned > al.state_updated_timestamp)
            });
            if child_changed {
                al.state_manually_set = false;
                out.changed = true;
            } else {
                frozen.insert(name.clone(), refs);
            }
        }
    }
    let mut unfrozen: Vec<String> = Vec::new();

    // Recompute composite states until stable (bounded by the composite count
    // so a reference cycle can't loop forever).
    for _ in 0..=composite_names.len() {
        // A child composite that changed earlier in this pass unfreezes its
        // manually set parent in the same pass.
        let thawed: Vec<String> = frozen
            .iter()
            .filter(|(_, refs)| {
                refs.iter().any(|r| {
                    members
                        .get(r)
                        .is_some_and(|m| states.get(r).is_some_and(|s| *s != m.state))
                })
            })
            .map(|(n, _)| n.clone())
            .collect();
        let mut changed = !thawed.is_empty();
        for n in thawed {
            frozen.remove(&n);
            unfrozen.push(n);
        }
        for name in &composite_names {
            if frozen.contains_key(name) {
                continue;
            }
            let Some(rule) = state
                .composite_alarms
                .get(region)
                .and_then(|c| c.get(name))
                .map(|a| a.alarm_rule.clone())
            else {
                continue;
            };
            if let Some(new_state) = evaluate_rule(&rule, &states) {
                if states.get(name) != Some(&new_state) {
                    states.insert(name.clone(), new_state);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    if let Some(c) = state.composite_alarms.get_mut(region) {
        for n in &unfrozen {
            if let Some(al) = c.get_mut(n) {
                al.state_manually_set = false;
                out.changed = true;
            }
        }
    }

    // Composites that change in this pass transition at `now`; a parent's
    // reason data reports that time for them.
    for name in &composite_names {
        if let (Some(new_state), Some(m)) = (states.get(name), members.get_mut(name)) {
            if *new_state != m.state {
                m.state = *new_state;
                m.transitioned = now;
            }
        }
    }

    // Apply the computed states and collect transitions.
    let mut history: Vec<AlarmHistoryItem> = Vec::new();
    if let Some(c) = state.composite_alarms.get_mut(region) {
        for (name, al) in c.iter_mut() {
            let Some(&new_state) = states.get(name) else {
                continue;
            };
            if new_state == al.state_value {
                continue;
            }
            let reason = format!(
                "The composite alarm rule evaluated to {}.",
                new_state.as_str()
            );
            let reason_data = parse_alarm_rule(&al.alarm_rule)
                .ok()
                .map(|node| triggering_alarms_data(&node, &members));
            history.push(state_update_item(
                name,
                "CompositeAlarm",
                now,
                StateSnapshot {
                    value: al.state_value,
                    reason: &al.state_reason,
                    reason_data: al.state_reason_data.as_deref(),
                },
                StateSnapshot {
                    value: new_state,
                    reason: &reason,
                    reason_data: reason_data.as_deref(),
                },
            ));
            // The actions for this transition are owed until the suppressor
            // lets them run (see `owed_composite_actions`).
            al.pending_action_from = Some(PreviousState {
                value: al.state_value,
                reason: std::mem::take(&mut al.state_reason),
                reason_data: al.state_reason_data.take(),
                timestamp: al.state_updated_timestamp,
            });
            al.alarm_exit.note(al.state_value, new_state, now);
            al.state_value = new_state;
            al.state_reason = reason;
            al.state_reason_data = reason_data;
            al.state_updated_timestamp = now;
            al.state_transitioned_timestamp = Some(now);
            out.changed = true;
        }
    }
    for item in history {
        crate::service::push_alarm_history(state, region, item);
    }

    if update_action_suppression(state, region, &composite_names, &states, now) {
        out.changed = true;
    }
    owed_composite_actions(state, &account_id, region, out);
}

/// Every alarm name a rule references.
fn referenced_names<'a>(node: &'a RuleNode, out: &mut Vec<&'a str>) {
    match node {
        RuleNode::And(a, b) | RuleNode::Or(a, b) => {
            referenced_names(a, out);
            referenced_names(b, out);
        }
        RuleNode::Not(a) => referenced_names(a, out),
        RuleNode::State(_, name) => out.push(name),
        RuleNode::True | RuleNode::False => {}
    }
}

/// Collect the actions composites owe for their last transition. A composite
/// in ALARM whose actions are suppressed keeps owing them and performs them
/// once the suppression ends (the wait period runs out, or the extension
/// period after the suppressor leaves ALARM), if it is still in ALARM.
fn owed_composite_actions(
    state: &mut CloudWatchState,
    account_id: &str,
    region: &str,
    out: &mut Evaluated,
) {
    let Some(c) = state.composite_alarms.get_mut(region) else {
        return;
    };
    for al in c.values_mut() {
        if al.pending_action_from.is_none()
            || (al.state_value == AlarmState::Alarm && al.actions_suppressed_by.is_some())
        {
            continue;
        }
        if let Some(prev) = al.pending_action_from.take() {
            out.changed = true;
            out.notifications
                .extend(crate::alarm_actions::composite_alarm_notification(
                    account_id, region, al, &prev,
                ));
        }
    }
}

/// The `StateReasonData` of a composite rule transition: every referenced
/// alarm whose state predicate currently holds, with its state and the time it
/// entered that state.
fn triggering_alarms_data(node: &RuleNode, members: &HashMap<String, RuleMember>) -> String {
    let mut names: Vec<&str> = Vec::new();
    collect_satisfied(node, members, &mut names);
    let mut seen = HashSet::new();
    let triggering: Vec<Value> = names
        .into_iter()
        .filter(|n| seen.insert(*n))
        .filter_map(|n| members.get(n))
        .map(|m| {
            json!({
                "arn": m.arn,
                "state": {
                    "value": m.state.as_str(),
                    "timestamp": reason_timestamp(m.transitioned),
                },
            })
        })
        .collect();
    json!({ "triggeringAlarms": triggering }).to_string()
}

fn collect_satisfied<'a>(
    node: &'a RuleNode,
    members: &HashMap<String, RuleMember>,
    out: &mut Vec<&'a str>,
) {
    match node {
        RuleNode::And(a, b) | RuleNode::Or(a, b) => {
            collect_satisfied(a, members, out);
            collect_satisfied(b, members, out);
        }
        RuleNode::Not(a) => collect_satisfied(a, members, out),
        RuleNode::State(want, name) => {
            if members.get(name).map(|m| m.state) == Some(*want) {
                out.push(name);
            }
        }
        RuleNode::True | RuleNode::False => {}
    }
}

/// Recompute `ActionsSuppressedBy` / `ActionsSuppressedReason` for every
/// composite alarm with an `ActionsSuppressor`. Returns whether any changed.
fn update_action_suppression(
    state: &mut CloudWatchState,
    region: &str,
    composite_names: &[String],
    states: &HashMap<String, AlarmState>,
    now: DateTime<Utc>,
) -> bool {
    let mut results: Vec<(String, Option<ActionsSuppressedBy>)> = Vec::new();
    if let Some(c) = state.composite_alarms.get(region) {
        for name in composite_names {
            let Some(al) = c.get(name) else {
                continue;
            };
            let by = al.actions_suppressor.as_deref().and_then(|s| {
                let suppressor = normalize_ref(s);
                suppression_status(&SuppressionInput {
                    composite_state: al.state_value,
                    entered_state_at: al
                        .state_transitioned_timestamp
                        .unwrap_or(al.state_updated_timestamp),
                    suppressor_state: states.get(&suppressor).copied(),
                    suppressor_left_alarm_at: suppressor_exit(state, region, &suppressor),
                    wait_period: al.actions_suppressor_wait_period.unwrap_or(0),
                    extension_period: al.actions_suppressor_extension_period.unwrap_or(0),
                    now,
                })
            });
            results.push((name.clone(), by));
        }
    }
    let mut changed = false;
    if let Some(c) = state.composite_alarms.get_mut(region) {
        for (name, by) in results {
            if let Some(al) = c.get_mut(&name) {
                if al.actions_suppressed_by != by {
                    changed = true;
                }
                al.actions_suppressed_by = by;
                al.actions_suppressed_reason =
                    by.map(|b| format!("Actions suppressed by {}", b.as_str()));
            }
        }
    }
    changed
}

/// When alarm `name` last left ALARM: recorded on the alarm, or read from its
/// history for an alarm persisted before that was tracked.
fn suppressor_exit(state: &CloudWatchState, region: &str, name: &str) -> Option<DateTime<Utc>> {
    let exit = state
        .alarms
        .get(region)
        .and_then(|a| a.get(name))
        .map(|a| a.alarm_exit)
        .or_else(|| {
            state
                .composite_alarms
                .get(region)
                .and_then(|a| a.get(name))
                .map(|a| a.alarm_exit)
        })
        .or_else(|| {
            state
                .log_alarms
                .get(region)
                .and_then(|a| a.get(name))
                .map(|a| a.alarm_exit)
        })?;
    if exit.tracked {
        exit.at
    } else {
        last_alarm_exit_from_history(state, region, name)
    }
}

/// The most recent time alarm `name` left the ALARM state, from its history.
fn last_alarm_exit_from_history(
    state: &CloudWatchState,
    region: &str,
    name: &str,
) -> Option<DateTime<Utc>> {
    state
        .alarm_history_in(region)?
        .get(name)?
        .iter()
        .filter(|i| i.history_item_type == "StateUpdate")
        .filter(|i| {
            let Ok(v) = serde_json::from_str::<Value>(&i.history_data) else {
                return false;
            };
            v["oldState"]["stateValue"] == "ALARM" && v["newState"]["stateValue"] != "ALARM"
        })
        .map(|i| i.timestamp)
        .max()
}

/// Inputs to [`suppression_status`].
pub(crate) struct SuppressionInput {
    pub composite_state: AlarmState,
    /// When the composite entered its current state.
    pub entered_state_at: DateTime<Utc>,
    /// The suppressor's current state (`None` when it doesn't exist).
    pub suppressor_state: Option<AlarmState>,
    pub suppressor_left_alarm_at: Option<DateTime<Utc>>,
    pub wait_period: i64,
    pub extension_period: i64,
    pub now: DateTime<Utc>,
}

/// Whether a composite alarm's ALARM actions are suppressed right now, and by
/// what:
///   * `Alarm` while the suppressor is in ALARM;
///   * `ExtensionPeriod` for `ActionsSuppressorExtensionPeriod` seconds after
///     the suppressor leaves ALARM;
///   * `WaitPeriod` for `ActionsSuppressorWaitPeriod` seconds after the
///     composite enters ALARM, while the suppressor hasn't gone into ALARM.
///
/// After those periods the composite performs its actions (`None`).
pub(crate) fn suppression_status(i: &SuppressionInput) -> Option<ActionsSuppressedBy> {
    if i.composite_state != AlarmState::Alarm {
        return None;
    }
    if i.suppressor_state == Some(AlarmState::Alarm) {
        return Some(ActionsSuppressedBy::Alarm);
    }
    // `start + seconds`, saturating to "never ends" if it overflows.
    let still_within = |start: DateTime<Utc>, seconds: i64| match Duration::try_seconds(seconds)
        .and_then(|d| start.checked_add_signed(d))
    {
        Some(end) => i.now < end,
        None => seconds > 0,
    };
    if let Some(left) = i.suppressor_left_alarm_at {
        if still_within(left, i.extension_period) {
            return Some(ActionsSuppressedBy::ExtensionPeriod);
        }
        // The suppressor went into ALARM during the wait, so the wait is over.
        if left >= i.entered_state_at {
            return None;
        }
    }
    if still_within(i.entered_state_at, i.wait_period) {
        return Some(ActionsSuppressedBy::WaitPeriod);
    }
    None
}

/// Whether [`evaluate_metric_alarm`] leaves `alarm` alone (metric-math,
/// anomaly-detection and PromQL alarms).
pub(crate) fn not_single_metric(alarm: &MetricAlarm) -> bool {
    !alarm.metrics.is_empty()
        || alarm.threshold_metric_id.is_some()
        || alarm.evaluation_criteria.is_some()
}

/// Why a single-metric alarm's configuration can't be evaluated, if it can't.
/// PutMetricAlarm accepts these shapes, so they surface at evaluation time as
/// `EVALUATION_ERROR` with the problem in `StateReason`.
fn configuration_error(alarm: &MetricAlarm) -> Option<String> {
    if alarm.namespace.is_none() {
        return Some("the alarm has no Namespace".to_string());
    }
    if alarm.metric_name.is_none() {
        return Some("the alarm has no MetricName".to_string());
    }
    if alarm.statistic.is_none() && alarm.extended_statistic.is_none() {
        return Some("the alarm has neither a Statistic nor an ExtendedStatistic".to_string());
    }
    if !matches!(
        alarm.comparison_operator.as_str(),
        "GreaterThanThreshold"
            | "GreaterThanOrEqualToThreshold"
            | "LessThanThreshold"
            | "LessThanOrEqualToThreshold"
    ) {
        return Some(format!(
            "the ComparisonOperator {} requires a ThresholdMetricId",
            alarm.comparison_operator
        ));
    }
    if alarm.threshold.is_none() {
        return Some("the alarm has no Threshold".to_string());
    }
    None
}

/// Compute the state a single-metric alarm should hold, or `None` to leave the
/// current state unchanged (metric-math / anomaly / PromQL alarms, or a metric
/// with no datapoints under `ignore`). `manually_set` is whether the current
/// state came from `SetAlarmState`; when true, a metric with no data under the
/// default missing-data treatment (or a configuration that can't be evaluated)
/// keeps its manual state instead of resetting to INSUFFICIENT_DATA.
/// `window(start, end)` returns the alarm's metric datapoints with timestamps
/// in `[start, end)`.
pub(crate) fn evaluate_metric_alarm<'a>(
    alarm: &MetricAlarm,
    window: impl FnOnce(DateTime<Utc>, DateTime<Utc>) -> Vec<&'a MetricDatum>,
    now: DateTime<Utc>,
    manually_set: bool,
) -> Option<MetricEvaluation> {
    // Only plain single-metric alarms with a static threshold are evaluated.
    // Metric-math (`Metrics`), anomaly-detection (`ThresholdMetricId`) and
    // PromQL (`EvaluationCriteria`) alarms are left untouched.
    if not_single_metric(alarm) {
        return None;
    }
    if let Some(problem) = configuration_error(alarm) {
        if manually_set {
            return None;
        }
        return Some(MetricEvaluation {
            state: AlarmState::InsufficientData,
            reason: format!("Unable to evaluate the alarm: {problem}."),
            reason_data: None,
            had_data: false,
            evaluation_state: Some(EvaluationState::EvaluationError),
        });
    }
    let threshold = alarm.threshold?;
    // A namespace is required for a single-metric alarm; the datapoints handed
    // in are already those for this alarm's namespace.
    alarm.namespace.as_ref()?;
    let metric_name = alarm.metric_name.as_ref()?;
    let stat = alarm
        .extended_statistic
        .clone()
        .or_else(|| alarm.statistic.clone())?;
    let period = alarm.period.unwrap_or(60).max(1);
    let eval_periods = alarm.evaluation_periods.max(1);
    let m = alarm.datapoints_to_alarm.unwrap_or(eval_periods).max(1);
    let treat = alarm
        .treat_missing_data
        .as_deref()
        .unwrap_or("missing")
        .to_ascii_lowercase();

    // The most recent period boundary at/just before `now`, then the
    // `eval_periods` slots ending there.
    let now_secs = now.timestamp();
    let latest = now_secs - now_secs.rem_euclid(period);
    let window_start = latest - (eval_periods - 1) * period;
    let window_end = latest + period; // exclusive; covers the latest bucket
    let start_ts = DateTime::<Utc>::from_timestamp(window_start, 0)?;
    let end_ts = DateTime::<Utc>::from_timestamp(window_end, 0)?;

    let buckets = collect_metric_buckets(
        window(start_ts, end_ts),
        metric_name,
        &alarm.dimensions,
        alarm.unit.as_deref(),
        period,
        start_ts,
        end_ts,
    );

    // Gather one value per slot (most recent first) plus the breaching flags,
    // and the `evaluatedDatapoints` / `recentDatapoints` of the reason data.
    let mut present = 0i64;
    let mut breaching = 0i64;
    let mut latest_value: Option<f64> = None;
    let mut evaluated: Vec<Value> = Vec::new();
    let mut recent: Vec<f64> = Vec::new();
    for i in 0..eval_periods {
        let slot_secs = latest - i * period;
        let Some(slot_ts) = DateTime::<Utc>::from_timestamp(slot_secs, 0) else {
            continue;
        };
        let value = buckets.get(&slot_ts).and_then(|bucket| {
            let mut sorted = bucket.samples.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            resolve_stat(&stat, bucket, &sorted).map(|v| (v, bucket.sample_count()))
        });
        match value {
            Some((v, sample_count)) => {
                present += 1;
                if latest_value.is_none() {
                    latest_value = Some(v);
                }
                if breaches(&alarm.comparison_operator, v, threshold) {
                    breaching += 1;
                }
                recent.push(v);
                evaluated.push(json!({
                    "timestamp": reason_timestamp(slot_ts),
                    "sampleCount": sample_count,
                    "value": v,
                }));
            }
            None => evaluated.push(json!({ "timestamp": reason_timestamp(slot_ts) })),
        }
    }
    recent.reverse();
    let missing = eval_periods - present;

    let mut data = Map::new();
    data.insert("version".into(), json!("1.0"));
    data.insert("queryDate".into(), json!(reason_timestamp(now)));
    data.insert("startDate".into(), json!(reason_timestamp(start_ts)));
    if let Some(u) = &alarm.unit {
        data.insert("unit".into(), json!(u));
    }
    data.insert("statistic".into(), json!(stat));
    data.insert("period".into(), json!(period));
    data.insert("recentDatapoints".into(), json!(recent));
    data.insert("threshold".into(), json!(threshold));
    data.insert("evaluatedDatapoints".into(), Value::Array(evaluated));
    let reason_data = Some(Value::Object(data).to_string());
    let outcome = |state: AlarmState, reason: String, had_data: bool| MetricEvaluation {
        state,
        reason,
        reason_data: reason_data.clone(),
        had_data,
        evaluation_state: None,
    };

    // No real datapoints in the evaluation window. How the alarm reacts depends
    // on treat_missing_data:
    //   * breaching    -> ALARM
    //   * notBreaching -> OK
    //   * ignore       -> keep the current state (None)
    //   * missing (default) -> INSUFFICIENT_DATA
    if present == 0 {
        return match treat.as_str() {
            "breaching" => Some(outcome(
                AlarmState::Alarm,
                "Insufficient data treated as breaching.".to_string(),
                false,
            )),
            "notbreaching" => Some(outcome(
                AlarmState::Ok,
                "Insufficient data treated as not breaching.".to_string(),
                false,
            )),
            "ignore" => None,
            // Default treatment: a metric with no datapoints across the window
            // transitions to INSUFFICIENT_DATA (so a stale OK/ALARM alarm does
            // not stay pinned to a value it can no longer justify). A state
            // forced via SetAlarmState is kept until real data arrives.
            _ if manually_set => None,
            _ => Some(outcome(
                AlarmState::InsufficientData,
                format!("Insufficient Data: {eval_periods} datapoints were unknown."),
                false,
            )),
        };
    }

    let effective_breaching = match treat.as_str() {
        "breaching" => breaching + missing,
        _ => breaching,
    };

    if effective_breaching >= m {
        let reason = format!(
            "Threshold Crossed: {effective_breaching} out of the last {eval_periods} datapoints \
             [{}] was {} the threshold ({threshold}).",
            latest_value.map(|v| v.to_string()).unwrap_or_default(),
            operator_phrase(&alarm.comparison_operator),
        );
        Some(outcome(AlarmState::Alarm, reason, true))
    } else {
        let reason = format!(
            "Threshold not crossed: {breaching} out of the last {eval_periods} datapoints was {} \
             the threshold ({threshold}).",
            operator_phrase(&alarm.comparison_operator),
        );
        Some(outcome(AlarmState::Ok, reason, true))
    }
}

/// Whether `value` breaches `threshold` under `comparison_operator`. Band
/// operators (which require a `ThresholdMetricId`) never breach here.
fn breaches(op: &str, value: f64, threshold: f64) -> bool {
    match op {
        "GreaterThanThreshold" => value > threshold,
        "GreaterThanOrEqualToThreshold" => value >= threshold,
        "LessThanThreshold" => value < threshold,
        "LessThanOrEqualToThreshold" => value <= threshold,
        _ => false,
    }
}

fn operator_phrase(op: &str) -> &'static str {
    match op {
        "GreaterThanThreshold" => "greater than",
        "GreaterThanOrEqualToThreshold" => "greater than or equal to",
        "LessThanThreshold" => "less than",
        "LessThanOrEqualToThreshold" => "less than or equal to",
        _ => "compared against",
    }
}

// ---------------------------------------------------------------------------
// Composite alarm rule parser + evaluator.
// ---------------------------------------------------------------------------

/// A parsed `AlarmRule` expression node.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RuleNode {
    And(Box<RuleNode>, Box<RuleNode>),
    Or(Box<RuleNode>, Box<RuleNode>),
    Not(Box<RuleNode>),
    /// A state predicate: the target alarm name must currently hold this state.
    State(AlarmState, String),
    True,
    False,
}

/// Evaluate a rule string against the current alarm states, returning the
/// composite's state (`ALARM` when the rule is true, otherwise `OK`), or `None`
/// if the rule doesn't parse.
fn evaluate_rule(rule: &str, states: &HashMap<String, AlarmState>) -> Option<AlarmState> {
    let node = parse_alarm_rule(rule).ok()?;
    Some(if eval_node(&node, states) {
        AlarmState::Alarm
    } else {
        AlarmState::Ok
    })
}

fn eval_node(node: &RuleNode, states: &HashMap<String, AlarmState>) -> bool {
    match node {
        RuleNode::And(a, b) => eval_node(a, states) && eval_node(b, states),
        RuleNode::Or(a, b) => eval_node(a, states) || eval_node(b, states),
        RuleNode::Not(a) => !eval_node(a, states),
        RuleNode::True => true,
        RuleNode::False => false,
        RuleNode::State(want, name) => states.get(name.as_str()) == Some(want),
    }
}

/// Parse an `AlarmRule` boolean expression. Returns an error string for a
/// malformed rule so `PutCompositeAlarm` can reject it.
pub(crate) fn parse_alarm_rule(rule: &str) -> Result<RuleNode, String> {
    let chars: Vec<char> = rule.chars().collect();
    let mut p = RuleParser {
        chars,
        pos: 0,
        depth: 0,
    };
    p.skip_ws();
    if p.pos >= p.chars.len() {
        return Err("empty alarm rule".to_string());
    }
    let node = p.parse_or()?;
    p.skip_ws();
    if p.pos != p.chars.len() {
        return Err(format!(
            "unexpected trailing input in alarm rule at {}",
            p.pos
        ));
    }
    Ok(node)
}

/// Maximum nesting depth for an `AlarmRule`. Guards against a stack overflow
/// on a pathologically nested rule (e.g. thousands of `(` or `NOT`); the
/// length cap alone (~10240 chars) still permits ~5000 nesting levels. Mirrors
/// the `metric_math` parser's depth guard.
const MAX_RULE_DEPTH: usize = 256;

struct RuleParser {
    chars: Vec<char>,
    pos: usize,
    depth: usize,
}

impl RuleParser {
    fn skip_ws(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    /// Peek an upper-cased keyword (a run of ASCII letters) without consuming.
    fn peek_keyword(&self) -> Option<String> {
        let mut i = self.pos;
        while i < self.chars.len() && self.chars[i].is_whitespace() {
            i += 1;
        }
        let start = i;
        while i < self.chars.len() && (self.chars[i].is_ascii_alphabetic() || self.chars[i] == '_')
        {
            i += 1;
        }
        if i == start {
            None
        } else {
            Some(
                self.chars[start..i]
                    .iter()
                    .collect::<String>()
                    .to_ascii_uppercase(),
            )
        }
    }

    fn consume_keyword(&mut self, kw: &str) {
        self.skip_ws();
        self.pos += kw.len();
    }

    fn parse_or(&mut self) -> Result<RuleNode, String> {
        let mut left = self.parse_and()?;
        while self.peek_keyword().as_deref() == Some("OR") {
            self.consume_keyword("OR");
            let right = self.parse_and()?;
            left = RuleNode::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<RuleNode, String> {
        let mut left = self.parse_unary()?;
        while self.peek_keyword().as_deref() == Some("AND") {
            self.consume_keyword("AND");
            let right = self.parse_unary()?;
            left = RuleNode::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<RuleNode, String> {
        if self.peek_keyword().as_deref() == Some("NOT") {
            self.consume_keyword("NOT");
            self.depth += 1;
            if self.depth > MAX_RULE_DEPTH {
                return Err("alarm rule nested too deeply".to_string());
            }
            let inner = self.parse_unary()?;
            self.depth -= 1;
            return Ok(RuleNode::Not(Box::new(inner)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<RuleNode, String> {
        self.skip_ws();
        match self.peek() {
            Some('(') => {
                self.pos += 1;
                self.depth += 1;
                if self.depth > MAX_RULE_DEPTH {
                    return Err("alarm rule nested too deeply".to_string());
                }
                let node = self.parse_or()?;
                self.depth -= 1;
                self.skip_ws();
                if self.peek() != Some(')') {
                    return Err("expected ')' in alarm rule".to_string());
                }
                self.pos += 1;
                Ok(node)
            }
            Some(_) => {
                let kw = self
                    .peek_keyword()
                    .ok_or_else(|| "expected a token in alarm rule".to_string())?;
                match kw.as_str() {
                    "TRUE" => {
                        self.consume_keyword("TRUE");
                        Ok(RuleNode::True)
                    }
                    "FALSE" => {
                        self.consume_keyword("FALSE");
                        Ok(RuleNode::False)
                    }
                    "ALARM" | "OK" | "INSUFFICIENT_DATA" => self.parse_func(&kw),
                    other => Err(format!("unexpected token '{other}' in alarm rule")),
                }
            }
            None => Err("unexpected end of alarm rule".to_string()),
        }
    }

    /// Parse `KIND ( ref )` where `ref` is a quoted string, an alarm name, or an
    /// alarm ARN.
    fn parse_func(&mut self, kind: &str) -> Result<RuleNode, String> {
        // `INSUFFICIENT_DATA` contains an underscore, so consume the exact
        // keyword text (letters + underscores) rather than a fixed length.
        self.skip_ws();
        while self.pos < self.chars.len()
            && (self.chars[self.pos].is_ascii_alphabetic() || self.chars[self.pos] == '_')
        {
            self.pos += 1;
        }
        self.skip_ws();
        if self.peek() != Some('(') {
            return Err(format!("expected '(' after {kind} in alarm rule"));
        }
        self.pos += 1;
        // Read the reference up to the closing paren.
        let start = self.pos;
        while self.pos < self.chars.len() && self.chars[self.pos] != ')' {
            self.pos += 1;
        }
        if self.peek() != Some(')') {
            return Err(format!("unterminated {kind}(...) in alarm rule"));
        }
        let raw: String = self.chars[start..self.pos].iter().collect();
        self.pos += 1; // consume ')'
        let name = normalize_ref(raw.trim());
        if name.is_empty() {
            return Err(format!("empty reference in {kind}(...)"));
        }
        let state = match kind {
            "ALARM" => AlarmState::Alarm,
            "OK" => AlarmState::Ok,
            "INSUFFICIENT_DATA" => AlarmState::InsufficientData,
            _ => unreachable!(),
        };
        Ok(RuleNode::State(state, name))
    }
}

/// Normalize an alarm reference: strip surrounding quotes and resolve an ARN to
/// its trailing alarm name.
fn normalize_ref(raw: &str) -> String {
    let unquoted = raw
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .or_else(|| raw.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
        .unwrap_or(raw)
        .trim();
    if let Some(idx) = unquoted.find(":alarm:") {
        return unquoted[idx + ":alarm:".len()..].to_string();
    }
    unquoted.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`super::evaluate_metric_alarm`] over a plain datapoint list (or none).
    fn evaluate_metric_alarm(
        alarm: &MetricAlarm,
        data: Option<&[MetricDatum]>,
        now: DateTime<Utc>,
        manually_set: bool,
    ) -> Option<MetricEvaluation> {
        super::evaluate_metric_alarm(
            alarm,
            |_, _| data.map(|d| d.iter().collect()).unwrap_or_default(),
            now,
            manually_set,
        )
    }

    fn states(pairs: &[(&str, AlarmState)]) -> HashMap<String, AlarmState> {
        pairs.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    #[test]
    fn parses_and_evaluates_simple_alarm_predicate() {
        let node = parse_alarm_rule("ALARM(a)").unwrap();
        assert!(eval_node(&node, &states(&[("a", AlarmState::Alarm)])));
        assert!(!eval_node(&node, &states(&[("a", AlarmState::Ok)])));
        // Missing referenced alarm -> predicate false.
        assert!(!eval_node(&node, &states(&[])));
    }

    #[test]
    fn evaluates_and_or_not_and_parens() {
        let node = parse_alarm_rule("(ALARM(a) AND ALARM(b)) OR NOT OK(c)").unwrap();
        let s = states(&[
            ("a", AlarmState::Alarm),
            ("b", AlarmState::Alarm),
            ("c", AlarmState::Ok),
        ]);
        // a AND b true -> whole true.
        assert!(eval_node(&node, &s));
        let s2 = states(&[
            ("a", AlarmState::Ok),
            ("b", AlarmState::Alarm),
            ("c", AlarmState::Alarm),
        ]);
        // a AND b false; NOT OK(c) -> c is ALARM not OK -> NOT false -> true.
        assert!(eval_node(&node, &s2));
    }

    #[test]
    fn quoted_and_arn_references_resolve() {
        let node = parse_alarm_rule("ALARM(\"my-alarm\")").unwrap();
        assert!(eval_node(
            &node,
            &states(&[("my-alarm", AlarmState::Alarm)])
        ));
        let node =
            parse_alarm_rule("ALARM(arn:aws:cloudwatch:us-east-1:123456789012:alarm:my-alarm)")
                .unwrap();
        assert!(eval_node(
            &node,
            &states(&[("my-alarm", AlarmState::Alarm)])
        ));
    }

    #[test]
    fn insufficient_data_predicate() {
        let node = parse_alarm_rule("INSUFFICIENT_DATA(a)").unwrap();
        assert!(eval_node(
            &node,
            &states(&[("a", AlarmState::InsufficientData)])
        ));
    }

    #[test]
    fn malformed_rules_error() {
        assert!(parse_alarm_rule("").is_err());
        assert!(parse_alarm_rule("ALARM(").is_err());
        assert!(parse_alarm_rule("ALARM(a) AND").is_err());
        assert!(parse_alarm_rule("BOGUS(a)").is_err());
        assert!(parse_alarm_rule("ALARM(a) garbage").is_err());
    }

    #[test]
    fn deeply_nested_rule_errors_without_stack_overflow() {
        // Thousands of nested parens / NOTs would blow the stack via unbounded
        // recursive descent; the depth guard rejects them with an error.
        let parens = format!("{}TRUE{}", "(".repeat(5000), ")".repeat(5000));
        assert!(parse_alarm_rule(&parens).is_err());
        let nots = format!("{}TRUE", "NOT ".repeat(5000));
        assert!(parse_alarm_rule(&nots).is_err());
        // A modestly nested rule still parses.
        assert!(parse_alarm_rule("((TRUE))").is_ok());
        assert!(parse_alarm_rule("NOT NOT FALSE").is_ok());
    }

    #[test]
    fn evaluate_rule_maps_to_alarm_or_ok() {
        let s = states(&[("a", AlarmState::Alarm)]);
        assert_eq!(evaluate_rule("ALARM(a)", &s), Some(AlarmState::Alarm));
        assert_eq!(evaluate_rule("OK(a)", &s), Some(AlarmState::Ok));
        assert_eq!(evaluate_rule("!!!", &s), None);
    }

    fn sum_alarm(state: AlarmState, treat: Option<&str>) -> MetricAlarm {
        MetricAlarm {
            alarm_name: "a".into(),
            alarm_arn: "arn:aws:cloudwatch:us-east-1:123456789012:alarm:a".into(),
            alarm_description: None,
            actions_enabled: true,
            ok_actions: vec![],
            alarm_actions: vec![],
            insufficient_data_actions: vec![],
            state_value: state,
            state_reason: String::new(),
            state_updated_timestamp: Utc::now(),
            metric_name: Some("M".into()),
            namespace: Some("Test/NS".into()),
            statistic: Some("Sum".into()),
            extended_statistic: None,
            dimensions: Default::default(),
            period: Some(60),
            unit: None,
            evaluation_periods: 1,
            datapoints_to_alarm: None,
            threshold: Some(1.0),
            comparison_operator: "GreaterThanThreshold".into(),
            treat_missing_data: treat.map(|s| s.to_string()),
            evaluate_low_sample_count_percentile: None,
            threshold_metric_id: None,
            configuration_updated_timestamp: Utc::now(),
            alarm_configuration_updated_timestamp: Utc::now(),
            metrics: vec![],
            state_manually_set: false,
            action_configuration: None,
            warm_up_configuration: None,
            evaluation_criteria: None,
            evaluation_window: None,
            evaluation_interval: None,
            state_reason_data: None,
            state_transitioned_timestamp: None,
            evaluation_state: None,
            alarm_exit: crate::state::AlarmExit::new(),
            manual_hold_until: None,
            next_evaluation_at: None,
        }
    }

    fn datum(value: f64, ts: DateTime<Utc>) -> MetricDatum {
        MetricDatum {
            metric_name: "M".into(),
            dimensions: Default::default(),
            timestamp: ts,
            value: Some(value),
            statistic_values: None,
            unit: None,
            storage_resolution: None,
        }
    }

    // Default (missing) treatment: a stale alarm with no datapoints transitions
    // to INSUFFICIENT_DATA rather than staying pinned to its old state.
    #[test]
    fn missing_data_default_transitions_to_insufficient_data() {
        let now = Utc::now();
        let alarm = sum_alarm(AlarmState::Alarm, None);
        let out = evaluate_metric_alarm(&alarm, None, now, false).unwrap();
        assert_eq!(out.state, AlarmState::InsufficientData);
        assert!(!out.had_data);
        assert_eq!(out.evaluation_state, None);
        // The reason data lists the unknown slot without a value.
        let data: Value = serde_json::from_str(out.reason_data.as_deref().unwrap()).unwrap();
        assert_eq!(data["recentDatapoints"], json!([]));
        let evaluated = data["evaluatedDatapoints"].as_array().unwrap();
        assert_eq!(evaluated.len(), 1);
        assert!(evaluated[0].get("value").is_none());
    }

    // `ignore` keeps the current state when data is missing.
    #[test]
    fn missing_data_ignore_keeps_state() {
        let now = Utc::now();
        let alarm = sum_alarm(AlarmState::Alarm, Some("ignore"));
        assert_eq!(evaluate_metric_alarm(&alarm, None, now, false), None);
    }

    // A manually-set state (SetAlarmState) is kept sticky when data is missing.
    #[test]
    fn manual_state_survives_missing_data() {
        let now = Utc::now();
        let alarm = sum_alarm(AlarmState::Alarm, None);
        assert_eq!(evaluate_metric_alarm(&alarm, None, now, true), None);
    }

    // Real datapoints govern the state and report had_data = true so the caller
    // can clear a stale manual override.
    #[test]
    fn present_data_drives_state_and_reports_had_data() {
        let now = Utc::now();
        let alarm = sum_alarm(AlarmState::InsufficientData, None);
        let data = [datum(5.0, now)];
        let out = evaluate_metric_alarm(&alarm, Some(&data), now, true).unwrap();
        assert_eq!(out.state, AlarmState::Alarm);
        assert!(out.had_data);
    }

    #[test]
    fn reason_data_reports_evaluated_datapoints() {
        let now = DateTime::<Utc>::from_timestamp(1_700_000_030, 0).unwrap();
        let mut alarm = sum_alarm(AlarmState::InsufficientData, None);
        alarm.evaluation_periods = 2;
        alarm.datapoints_to_alarm = Some(1);
        alarm.unit = Some("Count".into());
        let mut d1 = datum(2.0, now);
        d1.unit = Some("Count".into());
        let mut d2 = datum(3.0, now);
        d2.unit = Some("Count".into());
        let data = [d1, d2];
        let out = evaluate_metric_alarm(&alarm, Some(&data), now, false).unwrap();
        assert_eq!(out.state, AlarmState::Alarm);
        let v: Value = serde_json::from_str(out.reason_data.as_deref().unwrap()).unwrap();
        // Window: two 60s slots ending at the period boundary before `now`.
        assert_eq!(v["version"], "1.0");
        assert_eq!(v["queryDate"], "2023-11-14T22:13:50.000+0000");
        assert_eq!(v["startDate"], "2023-11-14T22:12:00.000+0000");
        assert_eq!(v["statistic"], "Sum");
        assert_eq!(v["unit"], "Count");
        assert_eq!(v["period"], 60);
        assert_eq!(v["threshold"], 1.0);
        assert_eq!(v["recentDatapoints"], json!([5.0]));
        assert_eq!(
            v["evaluatedDatapoints"],
            json!([
                {"timestamp": "2023-11-14T22:13:00.000+0000", "sampleCount": 2.0, "value": 5.0},
                {"timestamp": "2023-11-14T22:12:00.000+0000"},
            ])
        );
    }

    #[test]
    fn misconfigured_alarm_reports_evaluation_error() {
        let now = Utc::now();
        let mut alarm = sum_alarm(AlarmState::Ok, None);
        alarm.threshold = None;
        let out = evaluate_metric_alarm(&alarm, None, now, false).unwrap();
        assert_eq!(out.state, AlarmState::InsufficientData);
        assert_eq!(out.evaluation_state, Some(EvaluationState::EvaluationError));
        assert!(out.reason.contains("no Threshold"), "{}", out.reason);
        // A manual state is kept, as for missing data.
        assert_eq!(evaluate_metric_alarm(&alarm, None, now, true), None);
        // Metric-math / PromQL alarms are not evaluated here at all.
        alarm.evaluation_criteria = Some(Default::default());
        assert_eq!(evaluate_metric_alarm(&alarm, None, now, false), None);
    }

    #[test]
    fn apply_evaluation_separates_updated_and_transitioned() {
        let t0 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t1 = t0 + Duration::seconds(60);
        let t2 = t0 + Duration::seconds(120);
        let mut alarm = sum_alarm(AlarmState::Ok, None);
        alarm.state_updated_timestamp = t0;
        alarm.state_transitioned_timestamp = Some(t0);
        let mut history = Vec::new();
        let ev = |state, es| MetricEvaluation {
            state,
            reason: "r".into(),
            reason_data: Some("{\"version\":\"1.0\"}".into()),
            had_data: false,
            evaluation_state: es,
        };
        // Same value, same evaluation state: nothing moves.
        apply_metric_evaluation(&mut alarm, ev(AlarmState::Ok, None), t1, &mut history);
        assert_eq!(alarm.state_updated_timestamp, t0);
        assert!(history.is_empty());
        // Evaluation state changes without a value change: updated only.
        apply_metric_evaluation(
            &mut alarm,
            ev(AlarmState::Ok, Some(EvaluationState::EvaluationError)),
            t1,
            &mut history,
        );
        assert_eq!(alarm.state_updated_timestamp, t1);
        assert_eq!(alarm.state_transitioned_timestamp, Some(t0));
        assert!(history.is_empty());
        // Value changes: both move and a StateUpdate is recorded at that time.
        apply_metric_evaluation(&mut alarm, ev(AlarmState::Alarm, None), t2, &mut history);
        assert_eq!(alarm.state_updated_timestamp, t2);
        assert_eq!(alarm.state_transitioned_timestamp, Some(t2));
        assert_eq!(alarm.evaluation_state, None);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].timestamp, t2);
        let h: Value = serde_json::from_str(&history[0].history_data).unwrap();
        assert_eq!(h["oldState"]["stateValue"], "OK");
        assert_eq!(h["newState"]["stateValue"], "ALARM");
        assert_eq!(h["newState"]["stateReasonData"]["version"], "1.0");
    }

    fn suppression(
        composite: AlarmState,
        entered_ago: i64,
        suppressor: Option<AlarmState>,
        left_ago: Option<i64>,
    ) -> Option<ActionsSuppressedBy> {
        let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        suppression_status(&SuppressionInput {
            composite_state: composite,
            entered_state_at: now - Duration::seconds(entered_ago),
            suppressor_state: suppressor,
            suppressor_left_alarm_at: left_ago.map(|s| now - Duration::seconds(s)),
            wait_period: 120,
            extension_period: 60,
            now,
        })
    }

    #[test]
    fn suppression_follows_suppressor_and_periods() {
        use ActionsSuppressedBy::*;
        use AlarmState::{Alarm as A, Ok as O};
        // Composite not in ALARM: nothing to suppress.
        assert_eq!(suppression(O, 10, Some(A), None), None);
        // Suppressor in ALARM.
        assert_eq!(suppression(A, 10, Some(A), None), Some(Alarm));
        // Waiting for the suppressor, inside and past the wait period.
        assert_eq!(suppression(A, 10, Some(O), None), Some(WaitPeriod));
        assert_eq!(suppression(A, 500, Some(O), None), None);
        assert_eq!(suppression(A, 10, None, None), Some(WaitPeriod));
        // Suppressor left ALARM: extension period, then done (the wait ended
        // when the suppressor went into ALARM).
        assert_eq!(
            suppression(A, 100, Some(O), Some(30)),
            Some(ExtensionPeriod)
        );
        assert_eq!(suppression(A, 100, Some(O), Some(90)), None);
        // A suppressor exit long before the composite entered ALARM doesn't
        // end the wait.
        assert_eq!(suppression(A, 10, Some(O), Some(1000)), Some(WaitPeriod));
    }

    #[test]
    fn triggering_alarms_lists_satisfied_predicates() {
        let ts = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let mut members = HashMap::new();
        for (n, s) in [("a", AlarmState::Alarm), ("b", AlarmState::Ok)] {
            members.insert(
                n.to_string(),
                RuleMember {
                    arn: format!("arn:aws:cloudwatch:us-east-1:123456789012:alarm:{n}"),
                    state: s,
                    transitioned: ts,
                },
            );
        }
        let node = parse_alarm_rule("ALARM(a) OR ALARM(b) OR ALARM(a)").unwrap();
        let v: Value = serde_json::from_str(&triggering_alarms_data(&node, &members)).unwrap();
        assert_eq!(
            v,
            json!({"triggeringAlarms": [{
                "arn": "arn:aws:cloudwatch:us-east-1:123456789012:alarm:a",
                "state": {"value": "ALARM", "timestamp": "2023-11-14T22:13:20.000+0000"},
            }]})
        );
    }
}
