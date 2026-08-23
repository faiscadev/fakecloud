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

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::service::{collect_metric_buckets, resolve_stat};
use crate::state::{AlarmState, CloudWatchState, MetricAlarm, MetricDatum};

/// Evaluate every alarm in `region` against the stored metric data at `now`,
/// updating states and recording transitions in the alarm history.
pub(crate) fn evaluate_alarms(state: &mut CloudWatchState, region: &str, now: DateTime<Utc>) {
    // ---- Metric alarms ----
    let mut transitions: Vec<(String, String, AlarmState, String)> = Vec::new();
    {
        // Disjoint field borrows: `metrics` shared, `alarms` mutable.
        let data_map = state.metrics.get(region);
        if let Some(alarms) = state.alarms.get_mut(region) {
            for alarm in alarms.values_mut() {
                let data: Option<&[MetricDatum]> = alarm
                    .namespace
                    .as_ref()
                    .and_then(|ns| data_map.and_then(|m| m.get(ns)))
                    .map(|v| v.as_slice());
                let manually_set = alarm.state_manually_set;
                if let Some((new_state, reason, had_data)) =
                    evaluate_metric_alarm(alarm, data, now, manually_set)
                {
                    // Once real datapoints govern the alarm, a prior manual
                    // override no longer applies.
                    if had_data {
                        alarm.state_manually_set = false;
                    }
                    if new_state != alarm.state_value {
                        let old = alarm.state_value.as_str().to_string();
                        alarm.state_value = new_state;
                        alarm.state_reason = reason.clone();
                        alarm.state_updated_timestamp = now;
                        transitions.push((alarm.alarm_name.clone(), old, new_state, reason));
                    }
                }
            }
        }
    }
    for (name, old, new, reason) in &transitions {
        record_transition(state, region, name, "MetricAlarm", old, *new, reason, now);
    }

    // ---- Composite alarms ----
    evaluate_composite_alarms(state, region, now);
}

/// Evaluate the composite alarms in `region`, propagating child states through
/// the rule expressions. Runs to a fixpoint so a composite that references
/// another composite settles.
fn evaluate_composite_alarms(state: &mut CloudWatchState, region: &str, now: DateTime<Utc>) {
    let composite_names: Vec<String> = match state.composite_alarms.get(region) {
        Some(c) if !c.is_empty() => c.keys().cloned().collect(),
        _ => return,
    };

    // Seed the state map from all metric + composite alarms.
    let mut states: HashMap<String, AlarmState> = HashMap::new();
    if let Some(a) = state.alarms.get(region) {
        for (n, al) in a.iter() {
            states.insert(n.clone(), al.state_value);
        }
    }
    if let Some(c) = state.composite_alarms.get(region) {
        for (n, al) in c.iter() {
            states.insert(n.clone(), al.state_value);
        }
    }

    // Recompute composite states until stable (bounded by the composite count
    // so a reference cycle can't loop forever).
    for _ in 0..composite_names.len() {
        let mut changed = false;
        for name in &composite_names {
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

    // Apply the computed states and collect transitions.
    let mut transitions: Vec<(String, String, AlarmState)> = Vec::new();
    if let Some(c) = state.composite_alarms.get_mut(region) {
        for (name, al) in c.iter_mut() {
            let Some(&new_state) = states.get(name) else {
                continue;
            };
            if new_state != al.state_value {
                let old = al.state_value.as_str().to_string();
                al.state_value = new_state;
                al.state_reason = format!(
                    "The composite alarm rule evaluated to {}.",
                    new_state.as_str()
                );
                al.state_updated_timestamp = now;
                transitions.push((name.clone(), old, new_state));
            }
        }
    }
    for (name, old, new) in &transitions {
        let reason = format!("The composite alarm rule evaluated to {}.", new.as_str());
        record_transition(
            state,
            region,
            name,
            "CompositeAlarm",
            old,
            *new,
            &reason,
            now,
        );
    }
}

/// Compute the state a single-metric alarm should hold, or `None` to leave the
/// current state unchanged (metric-math / anomaly alarms, or a metric with no
/// datapoints under the default missing-data treatment).
/// Returns `Some((state, reason, had_data))` where `had_data` is whether the
/// evaluation window held any real datapoints, or `None` to leave the current
/// state unchanged. `manually_set` is whether the current state came from
/// `SetAlarmState`; when true, a metric with no data under the default
/// missing-data treatment keeps its manual state instead of resetting to
/// INSUFFICIENT_DATA.
pub(crate) fn evaluate_metric_alarm(
    alarm: &MetricAlarm,
    data: Option<&[MetricDatum]>,
    now: DateTime<Utc>,
    manually_set: bool,
) -> Option<(AlarmState, String, bool)> {
    // Only plain single-metric alarms with a static threshold are evaluated.
    // Metric-math (`Metrics`) and anomaly-detection (`ThresholdMetricId`)
    // alarms are left untouched.
    if !alarm.metrics.is_empty() || alarm.threshold_metric_id.is_some() {
        return None;
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

    let buckets = match data {
        Some(d) => collect_metric_buckets(
            d,
            metric_name,
            &alarm.dimensions,
            alarm.unit.as_deref(),
            period,
            start_ts,
            end_ts,
        ),
        None => Default::default(),
    };

    // Gather one value per slot (most recent first) plus the breaching flags.
    let mut present = 0i64;
    let mut breaching = 0i64;
    let mut latest_value: Option<f64> = None;
    for i in 0..eval_periods {
        let slot_secs = latest - i * period;
        let Some(slot_ts) = DateTime::<Utc>::from_timestamp(slot_secs, 0) else {
            continue;
        };
        if let Some(bucket) = buckets.get(&slot_ts) {
            let mut sorted = bucket.samples.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            if let Some(v) = resolve_stat(&stat, bucket, &sorted) {
                present += 1;
                if latest_value.is_none() {
                    latest_value = Some(v);
                }
                if breaches(&alarm.comparison_operator, v, threshold) {
                    breaching += 1;
                }
            }
        }
    }
    let missing = eval_periods - present;

    // No real datapoints in the evaluation window. How the alarm reacts depends
    // on treat_missing_data:
    //   * breaching    -> ALARM
    //   * notBreaching -> OK
    //   * ignore       -> keep the current state (None)
    //   * missing (default) -> INSUFFICIENT_DATA
    if present == 0 {
        return match treat.as_str() {
            "breaching" => Some((
                AlarmState::Alarm,
                "Insufficient data treated as breaching.".to_string(),
                false,
            )),
            "notbreaching" => Some((
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
            _ => Some((
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
        Some((AlarmState::Alarm, reason, true))
    } else {
        let reason = format!(
            "Threshold not crossed: {breaching} out of the last {eval_periods} datapoints was {} \
             the threshold ({threshold}).",
            operator_phrase(&alarm.comparison_operator),
        );
        Some((AlarmState::Ok, reason, true))
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

#[allow(clippy::too_many_arguments)]
fn record_transition(
    state: &mut CloudWatchState,
    region: &str,
    name: &str,
    alarm_type: &str,
    old: &str,
    new: AlarmState,
    reason: &str,
    _now: DateTime<Utc>,
) {
    let new_str = new.as_str();
    let summary = format!("Alarm updated from {old} to {new_str}");
    let history_data = format!(
        "{{\"oldState\":{{\"stateValue\":\"{old}\"}},\"newState\":{{\"stateValue\":\"{new_str}\",\"stateReason\":\"{}\"}}}}",
        reason.replace('"', "\\\"")
    );
    crate::service::push_alarm_history(
        state,
        region,
        name,
        alarm_type,
        "StateUpdate",
        summary,
        history_data,
    );
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
        let out = evaluate_metric_alarm(&alarm, None, now, false);
        assert!(matches!(
            out,
            Some((AlarmState::InsufficientData, _, false))
        ));
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
        let out = evaluate_metric_alarm(&alarm, Some(&data), now, true);
        assert!(matches!(out, Some((AlarmState::Alarm, _, true))), "{out:?}");
    }
}
