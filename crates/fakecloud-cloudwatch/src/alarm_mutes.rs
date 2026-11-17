//! Muting alarm actions with alarm mute rules.
//!
//! While a mute rule is active, the alarms it targets keep evaluating and
//! changing state (with history), but the actions of every state change are
//! held back: each muted change is recorded in the alarm history instead.
//! An alarm's mute window runs while any rule targeting it is active
//! (overlapping rules make one window). When the window ends (the schedule's
//! window closes, the rule expires, is deleted, or stops targeting the
//! alarm), CloudWatch compares the alarm's state with its state when the
//! window started: if they differ, the actions of the current state run;
//! otherwise none do, even if the alarm changed state and back during the
//! window. A composite alarm's owed actions then still go through its
//! `ActionsSuppressor`.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::alarm_actions::AlarmNotification;
use crate::state::{AlarmHistoryItem, AlarmState, CloudWatchState, PreviousState};

/// The active mute rules muting an alarm: the most recently created one, and
/// how many others.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutedBy {
    pub rule: String,
    pub others: usize,
}

/// What [`update_mutes`] found.
#[derive(Debug, Default)]
pub(crate) struct MuteUpdate {
    /// The alarms muted now.
    pub muted: BTreeMap<String, MutedBy>,
    /// The actions owed by metric and log alarms whose mute window just
    /// ended in a different state from the one it started in.
    pub owed: Vec<AlarmNotification>,
    /// Whether a mute window started or ended (state to persist).
    pub changed: bool,
}

/// The alarms of `region` the active mute rules mute at `now`.
pub(crate) fn active_mutes(
    state: &CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
) -> BTreeMap<String, MutedBy> {
    let mut by_alarm: BTreeMap<&str, Vec<(DateTime<Utc>, &str)>> = BTreeMap::new();
    if let Some(rules) = state.mute_rules.get(region) {
        for rule in rules.values() {
            if !crate::mute_schedule::rule_active(rule, now) {
                continue;
            }
            let created = rule
                .created_timestamp
                .unwrap_or(rule.last_updated_timestamp);
            for alarm in &rule.mute_target_alarm_names {
                by_alarm
                    .entry(alarm.as_str())
                    .or_default()
                    .push((created, rule.name.as_str()));
            }
        }
    }
    by_alarm
        .into_iter()
        .map(|(alarm, mut rules)| {
            rules.sort();
            rules.dedup_by(|a, b| a.1 == b.1);
            let (_, newest) = rules[rules.len() - 1];
            (
                alarm.to_string(),
                MutedBy {
                    rule: newest.to_string(),
                    others: rules.len() - 1,
                },
            )
        })
        .collect()
}

/// Whether `alarm` in `region` is muted at `now`.
pub fn alarm_muted(state: &CloudWatchState, region: &str, alarm: &str, now: DateTime<Utc>) -> bool {
    state.mute_rules.get(region).is_some_and(|rules| {
        rules.values().any(|r| {
            r.mute_target_alarm_names.iter().any(|n| n == alarm)
                && crate::mute_schedule::rule_active(r, now)
        })
    })
}

fn snapshot(
    value: AlarmState,
    reason: &str,
    reason_data: Option<&str>,
    at: DateTime<Utc>,
) -> PreviousState {
    PreviousState {
        value,
        reason: reason.to_string(),
        reason_data: reason_data.map(str::to_string),
        timestamp: at,
    }
}

/// Start and end the mute windows of `region`'s alarms at `now`: an alarm
/// muted now and not before records its current state; an alarm no longer
/// muted owes the actions of its current state if that differs from the
/// state its window started in (queued for composites as owed actions, which
/// their `ActionsSuppressor` still governs).
pub(crate) fn update_mutes(
    state: &mut CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
) -> MuteUpdate {
    let muted = active_mutes(state, region, now);
    let mut out = MuteUpdate::default();
    let has_windows = state.alarm_mutes.get(region).is_some_and(|m| !m.is_empty());
    if muted.is_empty() && !has_windows {
        return out;
    }
    let account_id = state.account_id.clone();
    let CloudWatchState {
        alarms,
        composite_alarms,
        log_alarms,
        alarm_mutes,
        ..
    } = state;
    let windows = alarm_mutes.entry(region.to_string()).or_default();
    let mut existing: BTreeSet<String> = BTreeSet::new();
    let mut changed = false;
    // Starts a window, or ends one and returns the state it started in.
    let mut step = |name: &str, current: PreviousState| -> Option<PreviousState> {
        if muted.contains_key(name) {
            if !windows.contains_key(name) {
                windows.insert(name.to_string(), current);
                changed = true;
            }
            None
        } else {
            let start = windows.remove(name);
            changed |= start.is_some();
            start
        }
    };
    if let Some(m) = alarms.get_mut(region) {
        for (name, al) in m.iter_mut() {
            existing.insert(name.clone());
            let current = snapshot(
                al.state_value,
                &al.state_reason,
                al.state_reason_data.as_deref(),
                al.state_updated_timestamp,
            );
            if let Some(start) = step(name, current) {
                out.owed
                    .extend(crate::alarm_actions::metric_alarm_notification(
                        &account_id,
                        region,
                        al,
                        &start,
                    ));
            }
        }
    }
    if let Some(m) = composite_alarms.get_mut(region) {
        for (name, al) in m.iter_mut() {
            existing.insert(name.clone());
            let current = snapshot(
                al.state_value,
                &al.state_reason,
                al.state_reason_data.as_deref(),
                al.state_transitioned_timestamp
                    .unwrap_or(al.state_updated_timestamp),
            );
            if let Some(start) = step(name, current) {
                if start.value != al.state_value {
                    al.pending_action_from = Some(start);
                } else {
                    // Same state as when the window started: nothing owed.
                    al.pending_action_from = None;
                }
            }
        }
    }
    if let Some(m) = log_alarms.get_mut(region) {
        for (name, al) in m.iter_mut() {
            existing.insert(name.clone());
            let current = snapshot(
                al.state_value,
                &al.state_reason,
                al.state_reason_data.as_deref(),
                al.state_updated_timestamp,
            );
            if let Some(start) = step(name, current) {
                out.owed
                    .extend(crate::alarm_actions::log_alarm_notification(
                        &account_id,
                        region,
                        al,
                        &start,
                    ));
            }
        }
    }
    // Windows of alarms that were deleted end with them.
    let before = windows.len();
    windows.retain(|name, _| existing.contains(name));
    changed |= windows.len() != before;
    if windows.is_empty() {
        alarm_mutes.remove(region);
    }
    out.changed = changed;
    out.muted = muted;
    out
}

/// Hold back the actions of muted alarms in `notifications`, recording each
/// muted state change in the alarm's history.
pub(crate) fn hold_muted(
    state: &mut CloudWatchState,
    region: &str,
    muted: &BTreeMap<String, MutedBy>,
    notifications: &mut Vec<AlarmNotification>,
    now: DateTime<Utc>,
) {
    if muted.is_empty() {
        return;
    }
    let mut held = Vec::new();
    notifications.retain(|n| match muted.get(&n.alarm_name) {
        Some(by) if n.region == region => {
            held.push(muted_item(n, by, now));
            false
        }
        _ => true,
    });
    for item in held {
        crate::service::push_alarm_history(state, region, item);
    }
}

/// The `Action` history item recording that a mute rule held back the
/// actions of a state change.
fn muted_item(n: &AlarmNotification, by: &MutedBy, now: DateTime<Utc>) -> AlarmHistoryItem {
    let summary = match by.others {
        0 => format!("Alarm actions muted by alarm mute rule {}", by.rule),
        1 => format!(
            "Alarm actions muted by alarm mute rule {} and 1 other active alarm mute rule",
            by.rule
        ),
        k => format!(
            "Alarm actions muted by alarm mute rule {} and {k} other active alarm mute rules",
            by.rule
        ),
    };
    let data = json!({
        "actionState": "Muted",
        "stateUpdateTimestamp": n.state_change_time.timestamp_millis(),
        "alarmMuteRuleName": by.rule,
        "otherActiveAlarmMuteRules": by.others,
        "mutedActions": n.actions,
    });
    AlarmHistoryItem {
        alarm_name: n.alarm_name.clone(),
        alarm_type: n.alarm_type.to_string(),
        timestamp: now,
        history_item_type: "Action".to_string(),
        history_summary: summary,
        history_data: data.to_string(),
    }
}
