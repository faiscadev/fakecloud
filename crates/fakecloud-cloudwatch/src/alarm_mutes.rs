//! Muting alarm actions with alarm mute rules.
//!
//! While a mute rule is active, the alarms it targets keep evaluating and
//! changing state (with history), but the actions of every state change are
//! held back: each muted change is recorded in the alarm history instead.
//! An alarm's mute window runs while any rule targeting it is active
//! (overlapping rules make one window).
//!
//! When the window ends because its schedule closes, the rule expires, or
//! the rule stops targeting the alarm, the CloudWatch User Guide ("How alarm
//! mute rules work") says:
//!
//! > When a mute window ends, CloudWatch compares the alarm's current state
//! > with the state the alarm was in when the mute window started. Actions
//! > are executed only if these two states are different, and the actions
//! > executed are those configured for the alarm's current state.
//!
//! and, for an alarm in ALARM at both ends of the window, "No ALARM action
//! is executed, because the alarm is in the same state (ALARM) as it was
//! when the mute window started."
//!
//! Deleting the rule follows the DeleteAlarmMuteRule API reference instead:
//!
//! > When you delete a mute rule, any alarms that are currently being muted
//! > by that rule are immediately unmuted. If those alarms are in an ALARM
//! > state, their configured actions will trigger.
//!
//! so an alarm a deleted rule unmutes runs its ALARM actions whenever it is
//! in ALARM, and otherwise follows the comparison above.
//!
//! A composite alarm's owed actions still go through its `ActionsSuppressor`;
//! actions the suppressor was already holding when the window started are
//! kept if nothing replaced them during the window.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::alarm_actions::AlarmNotification;
use crate::state::{AlarmHistoryItem, AlarmState, CloudWatchState, MuteWindow, PreviousState};

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
    /// The actions owed by alarms whose mute window just ended.
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
        .filter_map(|(alarm, mut rules)| {
            rules.sort();
            rules.dedup_by(|a, b| a.1 == b.1);
            let (_, newest) = *rules.last()?;
            Some((
                alarm.to_string(),
                MutedBy {
                    rule: newest.to_string(),
                    others: rules.len() - 1,
                },
            ))
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

/// How an alarm's window ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// Schedule closed, rule expired or retargeted: act only if the state
    /// differs from the window's start.
    Compare,
    /// A deleted rule unmuted the alarm: also act when it is in ALARM.
    Deleted,
}

/// Start and end the mute windows of `region`'s alarms at `now` (see the
/// module docs for what an ending window owes).
pub(crate) fn update_mutes(
    state: &mut CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
) -> MuteUpdate {
    update_mutes_releasing(state, region, now, &BTreeSet::new())
}

/// [`update_mutes`], where the windows of the alarms in `deleted` that end
/// now were ended by deleting the rule muting them.
pub(crate) fn update_mutes_releasing(
    state: &mut CloudWatchState,
    region: &str,
    now: DateTime<Utc>,
    deleted: &BTreeSet<String>,
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
    let mut changed = false;
    let ending = |name: &str| {
        if deleted.contains(name) {
            Ending::Deleted
        } else {
            Ending::Compare
        }
    };
    // Whether `value` at the end of a window started in `start` owes actions,
    // and whether they must run without a state change.
    let owes = |start: &PreviousState, value: AlarmState, ending: Ending| -> Option<bool> {
        if start.value != value {
            Some(false)
        } else if ending == Ending::Deleted && value == AlarmState::Alarm {
            Some(true)
        } else {
            None
        }
    };
    if let Some(m) = alarms.get_mut(region) {
        for (name, al) in m.iter_mut() {
            let is_muted = muted.contains_key(name);
            if is_muted && !windows.contains_key(name) {
                let start = snapshot(
                    al.state_value,
                    &al.state_reason,
                    al.state_reason_data.as_deref(),
                    al.state_updated_timestamp,
                );
                windows.insert(
                    name.clone(),
                    MuteWindow {
                        start,
                        composite_pending: None,
                    },
                );
                changed = true;
            } else if !is_muted {
                if let Some(w) = windows.remove(name) {
                    changed = true;
                    if let Some(same) = owes(&w.start, al.state_value, ending(name)) {
                        out.owed.extend(crate::alarm_actions::metric_notification(
                            &account_id,
                            region,
                            al,
                            &w.start,
                            !same,
                        ));
                    }
                }
            }
        }
    }
    if let Some(m) = composite_alarms.get_mut(region) {
        for (name, al) in m.iter_mut() {
            let is_muted = muted.contains_key(name);
            if is_muted && !windows.contains_key(name) {
                let start = snapshot(
                    al.state_value,
                    &al.state_reason,
                    al.state_reason_data.as_deref(),
                    al.state_transitioned_timestamp
                        .unwrap_or(al.state_updated_timestamp),
                );
                windows.insert(
                    name.clone(),
                    MuteWindow {
                        start,
                        composite_pending: al.pending_action_from.clone(),
                    },
                );
                changed = true;
            } else if !is_muted {
                let Some(w) = windows.remove(name) else {
                    continue;
                };
                changed = true;
                match owes(&w.start, al.state_value, ending(name)) {
                    // Owed for the change: the suppressor decides when.
                    Some(false) => al.pending_action_from = Some(w.start),
                    // A deleted rule and an alarm still in ALARM: its ALARM
                    // actions run now unless its suppressor holds them.
                    Some(true) if al.actions_suppressed_by.is_none() => {
                        al.pending_action_from = None;
                        out.owed
                            .extend(crate::alarm_actions::composite_notification(
                                &account_id,
                                region,
                                al,
                                &w.start,
                                false,
                            ));
                    }
                    // Back in the state the window started in: anything a
                    // transition during the window left pending is dropped,
                    // but an action the suppressor was already holding when
                    // the window started stays owed.
                    _ => {
                        if al.pending_action_from != w.composite_pending {
                            al.pending_action_from = None;
                        }
                    }
                }
            }
        }
    }
    if let Some(m) = log_alarms.get_mut(region) {
        for (name, al) in m.iter_mut() {
            let is_muted = muted.contains_key(name);
            if is_muted && !windows.contains_key(name) {
                let start = snapshot(
                    al.state_value,
                    &al.state_reason,
                    al.state_reason_data.as_deref(),
                    al.state_updated_timestamp,
                );
                windows.insert(
                    name.clone(),
                    MuteWindow {
                        start,
                        composite_pending: None,
                    },
                );
                changed = true;
            } else if !is_muted {
                if let Some(w) = windows.remove(name) {
                    changed = true;
                    if let Some(same) = owes(&w.start, al.state_value, ending(name)) {
                        out.owed.extend(crate::alarm_actions::log_notification(
                            &account_id,
                            region,
                            al,
                            &w.start,
                            !same,
                        ));
                    }
                }
            }
        }
    }
    // Windows of alarms that were deleted end with them.
    let exists = |name: &String| {
        alarms.get(region).is_some_and(|m| m.contains_key(name))
            || composite_alarms
                .get(region)
                .is_some_and(|m| m.contains_key(name))
            || log_alarms.get(region).is_some_and(|m| m.contains_key(name))
    };
    let before = windows.len();
    windows.retain(|name, _| exists(name));
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
