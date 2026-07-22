use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, Timelike, Utc};
use serde_json::json;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::SharedLambdaState;
use fakecloud_logs::SharedLogsState;
use fakecloud_persistence::SnapshotStore;

use crate::state::SharedEventBridgeState;

/// Parsed schedule expression.
enum Schedule {
    /// Rate-based: fire every `interval` duration.
    Rate(Duration),
    /// Cron-based: `cron(min hour dom month dow year)`.
    Cron(CronExpr),
}

/// A cron expression with 6 fields: min hour dom month dow year.
struct CronExpr {
    minute: CronField,
    hour: CronField,
    day_of_month: CronField,
    month: CronField,
    day_of_week: CronField,
    year: CronField,
}

#[derive(Debug, Clone, PartialEq)]
enum CronField {
    /// `*` or `?` — matches any value.
    Any,
    /// Comma-separated list of terms; matches when ANY term matches.
    Terms(Vec<CronTerm>),
}

/// One comma-separated element of a cron field.
#[derive(Debug, Clone, PartialEq)]
enum CronTerm {
    Single(u32),
    Range(u32, u32),
    Step { start: u32, end: u32, step: u32 },
}

/// Field bounds + name aliases used to parse and validate one cron field.
struct FieldSpec {
    min: u32,
    max: u32,
    names: &'static [(&'static str, u32)],
}

const MONTH_NAMES: &[(&str, u32)] = &[
    ("JAN", 1),
    ("FEB", 2),
    ("MAR", 3),
    ("APR", 4),
    ("MAY", 5),
    ("JUN", 6),
    ("JUL", 7),
    ("AUG", 8),
    ("SEP", 9),
    ("OCT", 10),
    ("NOV", 11),
    ("DEC", 12),
];

// Day-of-week names normalized to 0=Sunday..6=Saturday (chrono's
// num_days_from_sunday space).
const DOW_NAMES: &[(&str, u32)] = &[
    ("SUN", 0),
    ("MON", 1),
    ("TUE", 2),
    ("WED", 3),
    ("THU", 4),
    ("FRI", 5),
    ("SAT", 6),
];

fn resolve_token(tok: &str, spec: &FieldSpec, is_dow: bool) -> Option<u32> {
    let tok = tok.trim();
    if let Ok(n) = tok.parse::<u32>() {
        if is_dow {
            // AWS EventBridge day-of-week: 1=SUN..7=SAT -> 0=SUN..6=SAT.
            return match n {
                0 => Some(0),
                1..=7 => Some(n - 1),
                _ => None,
            };
        }
        return Some(n);
    }
    let upper = tok.to_ascii_uppercase();
    spec.names
        .iter()
        .find(|(name, _)| upper.starts_with(name))
        .map(|&(_, v)| v)
}

fn parse_schedule(expr: &str) -> Option<Schedule> {
    let expr = expr.trim();
    if let Some(inner) = expr.strip_prefix("rate(").and_then(|s| s.strip_suffix(')')) {
        return parse_rate(inner.trim());
    }
    if let Some(inner) = expr.strip_prefix("cron(").and_then(|s| s.strip_suffix(')')) {
        return parse_cron(inner.trim());
    }
    None
}

/// Returns whether `expr` is a syntactically valid EventBridge schedule
/// expression (`rate(...)` or `cron(...)`). Used by `PutRule` to reject a
/// malformed `ScheduleExpression` with a `ValidationException` instead of
/// silently storing a rule that never fires.
pub(crate) fn is_valid_schedule_expression(expr: &str) -> bool {
    parse_schedule(expr).is_some()
}

fn parse_rate(inner: &str) -> Option<Schedule> {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }
    let value: u64 = parts[0].parse().ok()?;
    // AWS requires a positive value: `rate(0 ...)` is rejected.
    if value == 0 {
        return None;
    }
    // The unit must be singular iff the value is 1 (`rate(1 minute)` /
    // `rate(5 minutes)`); a singular/plural mismatch is rejected.
    let (secs_per, singular) = match parts[1] {
        "second" => (1, true),
        "seconds" => (1, false),
        "minute" => (60, true),
        "minutes" => (60, false),
        "hour" => (3600, true),
        "hours" => (3600, false),
        "day" => (86400, true),
        "days" => (86400, false),
        _ => return None,
    };
    if singular != (value == 1) {
        return None;
    }
    Some(Schedule::Rate(Duration::from_secs(value * secs_per)))
}

fn parse_cron(inner: &str) -> Option<Schedule> {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 6 {
        return None;
    }
    // AWS EventBridge field bounds. Any field failing to parse rejects the
    // whole expression so PutRule surfaces a validation error rather than
    // silently accepting a rule that fires at the wrong times (bug-audit
    // 2026-05-28, 1.2).
    Some(Schedule::Cron(CronExpr {
        minute: parse_cron_field(
            parts[0],
            &FieldSpec {
                min: 0,
                max: 59,
                names: &[],
            },
            false,
        )?,
        hour: parse_cron_field(
            parts[1],
            &FieldSpec {
                min: 0,
                max: 23,
                names: &[],
            },
            false,
        )?,
        day_of_month: parse_cron_field(
            parts[2],
            &FieldSpec {
                min: 1,
                max: 31,
                names: &[],
            },
            false,
        )?,
        month: parse_cron_field(
            parts[3],
            &FieldSpec {
                min: 1,
                max: 12,
                names: MONTH_NAMES,
            },
            false,
        )?,
        // Day-of-week normalized to 0=SUN..6=SAT during parsing.
        day_of_week: parse_cron_field(
            parts[4],
            &FieldSpec {
                min: 0,
                max: 6,
                names: DOW_NAMES,
            },
            true,
        )?,
        year: parse_cron_field(
            parts[5],
            &FieldSpec {
                min: 1970,
                max: 2199,
                names: &[],
            },
            false,
        )?,
    }))
}

/// Parse one cron field: `*`/`?`, single values, ranges (`9-17`), lists
/// (`0,30`), steps (`*/5`, `9-17/2`), and name aliases. Returns `None`
/// (rejecting the whole expression) for any malformed or out-of-range
/// field — previously ranges/lists/steps/names were coerced to wildcard,
/// so a rule would fire at the wrong times (bug-audit 2026-05-28, 1.2).
fn parse_cron_field(s: &str, spec: &FieldSpec, is_dow: bool) -> Option<CronField> {
    let s = s.trim();
    if s == "*" || s == "?" {
        return Some(CronField::Any);
    }
    let mut terms = Vec::new();
    for part in s.split(',') {
        terms.push(parse_cron_term(part.trim(), spec, is_dow)?);
    }
    if terms.is_empty() {
        return None;
    }
    Some(CronField::Terms(terms))
}

fn parse_cron_term(part: &str, spec: &FieldSpec, is_dow: bool) -> Option<CronTerm> {
    let in_bounds = |v: u32| v >= spec.min && v <= spec.max;
    if let Some((base, step_s)) = part.split_once('/') {
        let step: u32 = step_s.trim().parse().ok()?;
        if step == 0 {
            return None;
        }
        let (start, end) = if base == "*" {
            (spec.min, spec.max)
        } else if let Some((a, b)) = base.split_once('-') {
            (
                resolve_token(a, spec, is_dow)?,
                resolve_token(b, spec, is_dow)?,
            )
        } else {
            (resolve_token(base, spec, is_dow)?, spec.max)
        };
        if !in_bounds(start) || !in_bounds(end) || start > end {
            return None;
        }
        return Some(CronTerm::Step { start, end, step });
    }
    if let Some((a, b)) = part.split_once('-') {
        let start = resolve_token(a, spec, is_dow)?;
        let end = resolve_token(b, spec, is_dow)?;
        if !in_bounds(start) || !in_bounds(end) || start > end {
            return None;
        }
        return Some(CronTerm::Range(start, end));
    }
    let v = resolve_token(part, spec, is_dow)?;
    if !in_bounds(v) {
        return None;
    }
    Some(CronTerm::Single(v))
}

#[allow(clippy::manual_is_multiple_of)] // keep `% step == 0` for MSRV
fn term_matches(term: &CronTerm, value: u32) -> bool {
    match term {
        CronTerm::Single(v) => *v == value,
        CronTerm::Range(a, b) => value >= *a && value <= *b,
        CronTerm::Step { start, end, step } => {
            value >= *start && value <= *end && (value - start).is_multiple_of(*step)
        }
    }
}

fn matches_field(field: &CronField, value: u32) -> bool {
    match field {
        CronField::Any => true,
        CronField::Terms(terms) => terms.iter().any(|t| term_matches(t, value)),
    }
}

fn cron_matches_now(cron: &CronExpr) -> bool {
    cron_matches_at(cron, Utc::now())
}

fn cron_matches_at(cron: &CronExpr, now: chrono::DateTime<Utc>) -> bool {
    matches_field(&cron.minute, now.minute())
        && matches_field(&cron.hour, now.hour())
        && matches_field(&cron.day_of_month, now.day())
        && matches_field(&cron.month, now.month())
        && matches_field(&cron.day_of_week, now.weekday().num_days_from_sunday())
        && matches_field(&cron.year, now.year() as u32)
}

/// Background scheduler that fires scheduled EventBridge rules.
pub struct Scheduler {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
    lambda_state: Option<SharedLambdaState>,
    logs_state: Option<SharedLogsState>,
    /// Persist hook for CloudWatch Logs, fired after a scheduled rule delivers
    /// to a Logs target. The rule scheduler is the pure side-channel: it fires
    /// on a timer with no request-path snapshot, so without writing through
    /// here the delivered `LogEvent` would vanish on the next restart.
    logs_persist: Option<fakecloud_persistence::SnapshotHook>,
    container_runtime: Option<Arc<ContainerRuntime>>,
    /// Persist hook. Firing a rule advances its `last_fired` directly, outside
    /// the service's action-dispatch path that is otherwise the only thing that
    /// snapshots -- so without writing through here the on-disk `last_fired`
    /// stays at its creation value, and a `rate(...)` rule double-fires on the
    /// next restart (`last_fired == None` => fire immediately). `None` in
    /// memory-only mode.
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl Scheduler {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            lambda_state: None,
            logs_state: None,
            logs_persist: None,
            container_runtime: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Wire the snapshot store so each tick that advances a rule's `last_fired`
    /// is written through to disk (see the `snapshot_store` field).
    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_lambda(mut self, lambda_state: SharedLambdaState) -> Self {
        self.lambda_state = Some(lambda_state);
        self
    }

    pub fn with_logs(mut self, logs_state: SharedLogsState) -> Self {
        self.logs_state = Some(logs_state);
        self
    }

    /// Wire the CloudWatch Logs persist hook so a scheduled rule delivering to
    /// a Logs target writes the event through to the Logs snapshot.
    pub fn with_logs_persist(mut self, hook: fakecloud_persistence::SnapshotHook) -> Self {
        self.logs_persist = Some(hook);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<ContainerRuntime>) -> Self {
        self.container_runtime = Some(runtime);
        self
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        // Track last-fired-minute for cron to avoid firing multiple times in the same minute
        // Keyed by (bus_name, rule_name) to distinguish same-named rules on different buses
        let mut cron_last_minute: HashMap<crate::state::RuleKey, (u32, u32)> = HashMap::new();

        loop {
            interval.tick().await;
            self.tick_and_persist(&mut cron_last_minute).await;
        }
    }

    /// Run one tick and, when it advanced any rule's `last_fired`, write the
    /// EventBridge snapshot through so the advance survives a restart. Only
    /// persists on a mutating tick, to avoid a snapshot every idle second.
    async fn tick_and_persist(
        &self,
        cron_last_minute: &mut HashMap<crate::state::RuleKey, (u32, u32)>,
    ) {
        if self.tick(cron_last_minute) {
            crate::service::save_eventbridge_snapshot(
                &self.state,
                self.snapshot_store.clone(),
                &self.snapshot_lock,
            )
            .await;
        }
    }

    /// Run one firing pass. Returns `true` if it advanced any rule's
    /// `last_fired` (persisted state), so the caller knows to write the
    /// snapshot through.
    fn tick(&self, cron_last_minute: &mut HashMap<crate::state::RuleKey, (u32, u32)>) -> bool {
        let now = Utc::now();

        // Collect rules that need to fire (to avoid holding lock during delivery)
        // Each entry includes the account_id that owns the rule and the rule
        // ARN (so InputTransformer can resolve `<aws.events.rule-arn>`).
        let mut to_fire: Vec<(
            String,
            String,
            String,
            String,
            Vec<crate::state::EventTarget>,
        )> = Vec::new();

        {
            let mut accounts = self.state.write();
            for (account_id, state) in accounts.iter_mut() {
                let account_id = account_id.to_string();
                let region = state.region.clone();
                let rule_keys: Vec<crate::state::RuleKey> = state.rules.keys().cloned().collect();

                for key in rule_keys {
                    let rule = match state.rules.get(&key) {
                        Some(r) => r,
                        None => continue,
                    };
                    let name = rule.name.clone();

                    if rule.state != "ENABLED" {
                        continue;
                    }

                    let schedule_expr = match &rule.schedule_expression {
                        Some(s) => s.clone(),
                        None => continue,
                    };

                    if rule.targets.is_empty() {
                        continue;
                    }

                    let schedule = match parse_schedule(&schedule_expr) {
                        Some(s) => s,
                        None => continue,
                    };

                    let should_fire = match &schedule {
                        Schedule::Rate(duration) => match rule.last_fired {
                            Some(last) => {
                                let elapsed = now.signed_duration_since(last);
                                elapsed.to_std().unwrap_or(Duration::ZERO) >= *duration
                            }
                            None => true, // Never fired, fire immediately
                        },
                        Schedule::Cron(cron) => {
                            if !cron_matches_now(cron) {
                                false
                            } else {
                                // Avoid firing multiple times in the same minute
                                let current = (now.hour(), now.minute());
                                let last = cron_last_minute.get(&key);
                                if last == Some(&current) {
                                    false
                                } else {
                                    cron_last_minute.insert(key.clone(), current);
                                    true
                                }
                            }
                        }
                    };

                    if should_fire {
                        let targets = rule.targets.clone();
                        let rule_arn = rule.arn.clone();
                        // Update last_fired while we hold the write lock
                        if let Some(r) = state.rules.get_mut(&key) {
                            r.last_fired = Some(now);
                        }
                        to_fire.push((account_id.clone(), region.clone(), name, rule_arn, targets));
                    }
                }
            }
        }
        // Lock is dropped here

        // A non-empty fire set means at least one rule's `last_fired` was
        // advanced under the write lock above; that persisted change must be
        // written through by the caller.
        let mutated = !to_fire.is_empty();

        // Deliver events. Reuse the shared single-target dispatch so scheduled
        // rules honour the same target shape as PutEvents-driven rules —
        // Input / InputPath / InputTransformer resolution plus the Kinesis,
        // api-destination and FIFO MessageGroupId branches — instead of the
        // scheduler's previous reduced delivery path.
        for (account_id, region, rule_name, rule_arn, targets) in to_fire {
            let event_id = uuid::Uuid::new_v4().to_string();
            let event_json = json!({
                "version": "0",
                "id": event_id,
                "source": "aws.events",
                "account": account_id,
                "detail-type": "Scheduled Event",
                "detail": {},
                "time": now.to_rfc3339(),
                "region": region,
                "resources": [],
            });

            tracing::debug!(rule = %rule_name, targets = targets.len(), "scheduler firing");

            let ctx = crate::service::EventDispatchContext {
                state: &self.state,
                delivery: &self.delivery,
                lambda_state: self.lambda_state.as_ref(),
                logs_state: self.logs_state.as_ref(),
                logs_persist: self.logs_persist.as_ref(),
                container_runtime: &self.container_runtime,
                account_id: &account_id,
                region: &region,
            };
            for target in &targets {
                crate::service::dispatch_event_target(
                    &ctx,
                    target,
                    &event_json,
                    &event_id,
                    "Scheduled Event",
                    Some(&rule_arn),
                );
            }
        }

        mutated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn parse_rate_minutes() {
        let s = parse_schedule("rate(5 minutes)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(300)));
    }

    #[test]
    fn parse_rate_singular() {
        let s = parse_schedule("rate(1 hour)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(3600)));
    }

    #[test]
    fn parse_rate_seconds() {
        let s = parse_schedule("rate(1 second)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(1)));
    }

    #[test]
    fn parse_rate_days() {
        let s = parse_schedule("rate(2 days)");
        assert!(matches!(s, Some(Schedule::Rate(d)) if d == Duration::from_secs(172800)));
    }

    #[test]
    fn parse_cron_all_wildcards() {
        let s = parse_schedule("cron(* * * * ? *)");
        assert!(matches!(s, Some(Schedule::Cron(_))));
    }

    #[test]
    fn parse_cron_specific_values() {
        let s = parse_schedule("cron(0 12 * * ? *)");
        match s {
            Some(Schedule::Cron(c)) => {
                assert_eq!(c.minute, CronField::Terms(vec![CronTerm::Single(0)]));
                assert_eq!(c.hour, CronField::Terms(vec![CronTerm::Single(12)]));
                assert_eq!(c.day_of_month, CronField::Any);
                assert_eq!(c.month, CronField::Any);
                assert_eq!(c.day_of_week, CronField::Any);
            }
            _ => panic!("expected cron"),
        }
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(parse_schedule("invalid").is_none());
        assert!(parse_schedule("rate()").is_none());
        assert!(parse_schedule("rate(abc minutes)").is_none());
        assert!(parse_schedule("cron(1 2 3)").is_none());
    }

    #[test]
    fn parse_rate_zero_is_rejected() {
        // AWS requires a positive value; `rate(0 ...)` is invalid.
        assert!(parse_schedule("rate(0 seconds)").is_none());
        assert!(parse_schedule("rate(0 minutes)").is_none());
    }

    #[test]
    fn parse_rate_singular_plural_mismatch_rejected() {
        // Unit must be singular iff value == 1.
        assert!(parse_schedule("rate(1 minutes)").is_none());
        assert!(parse_schedule("rate(5 minute)").is_none());
        assert!(parse_schedule("rate(1 minute)").is_some());
        assert!(parse_schedule("rate(5 minutes)").is_some());
    }

    #[test]
    fn parse_rate_unknown_unit_rejected() {
        assert!(parse_schedule("rate(1 fortnight)").is_none());
    }

    #[test]
    fn parse_cron_question_mark_is_any() {
        let s = parse_schedule("cron(? ? ? ? ? ?)");
        assert!(matches!(s, Some(Schedule::Cron(_))));
    }

    #[test]
    fn parse_cron_non_numeric_field_is_rejected() {
        // bug-audit 2026-05-28, 1.2: unknown tokens must reject the whole
        // expression, not coerce to wildcard (which fired every minute).
        assert!(parse_schedule("cron(xyz 12 * * ? *)").is_none());
    }

    #[test]
    fn cron_wildcard_always_matches() {
        let cron = CronExpr {
            minute: CronField::Any,
            hour: CronField::Any,
            day_of_month: CronField::Any,
            month: CronField::Any,
            day_of_week: CronField::Any,
            year: CronField::Any,
        };
        assert!(cron_matches_now(&cron));
    }

    #[test]
    fn cron_impossible_minute_never_matches() {
        let cron = CronExpr {
            minute: CronField::Terms(vec![CronTerm::Single(99)]),
            hour: CronField::Any,
            day_of_month: CronField::Any,
            month: CronField::Any,
            day_of_week: CronField::Any,
            year: CronField::Any,
        };
        assert!(!cron_matches_now(&cron));
    }

    // bug-audit 2026-05-28, 1.2: ranges/lists/steps/names fire only at the
    // right times; numeric DOW is AWS one-based-Sunday; year is enforced.
    fn at(min: u32, hour: u32, dom: u32, month: u32, year: i32) -> chrono::DateTime<Utc> {
        use chrono::TimeZone;
        Utc.with_ymd_and_hms(year, month, dom, hour, min, 0)
            .unwrap()
    }

    #[test]
    fn cron_step_minute_fires_only_on_multiples() {
        let Some(Schedule::Cron(c)) = parse_schedule("cron(*/5 * * * ? *)") else {
            panic!("expected cron");
        };
        assert!(cron_matches_at(&c, at(0, 9, 1, 6, 2026)));
        assert!(cron_matches_at(&c, at(5, 9, 1, 6, 2026)));
        assert!(!cron_matches_at(&c, at(3, 9, 1, 6, 2026)));
    }

    #[test]
    fn cron_hour_range_and_dow_names() {
        // 2026-06-01 = Monday, 2026-06-06 = Saturday.
        let Some(Schedule::Cron(c)) = parse_schedule("cron(0 9-17 ? * MON-FRI *)") else {
            panic!("expected cron");
        };
        assert!(cron_matches_at(&c, at(0, 9, 1, 6, 2026)));
        assert!(cron_matches_at(&c, at(0, 17, 1, 6, 2026)));
        assert!(!cron_matches_at(&c, at(0, 8, 1, 6, 2026)));
        assert!(!cron_matches_at(&c, at(0, 9, 6, 6, 2026)));
    }

    #[test]
    fn cron_list_and_month_name_and_year() {
        let Some(Schedule::Cron(c)) = parse_schedule("cron(0,30 0 1 JAN ? 2027)") else {
            panic!("expected cron");
        };
        assert!(cron_matches_at(&c, at(0, 0, 1, 1, 2027)));
        assert!(cron_matches_at(&c, at(30, 0, 1, 1, 2027)));
        assert!(!cron_matches_at(&c, at(15, 0, 1, 1, 2027))); // not in list
        assert!(!cron_matches_at(&c, at(0, 0, 1, 1, 2026))); // wrong year
    }

    #[test]
    fn cron_numeric_dow_is_one_based_sunday() {
        // AWS: 2 = Monday. 2026-06-01 is a Monday.
        let Some(Schedule::Cron(c)) = parse_schedule("cron(0 0 ? * 2 *)") else {
            panic!("expected cron");
        };
        assert!(cron_matches_at(&c, at(0, 0, 1, 6, 2026)));
        assert!(!cron_matches_at(&c, at(0, 0, 2, 6, 2026)));
    }

    #[test]
    fn cron_invalid_fields_rejected() {
        assert!(parse_schedule("cron(99 * * * ? *)").is_none());
        assert!(parse_schedule("cron(0 25 * * ? *)").is_none());
        assert!(parse_schedule("cron(0 0 1 13 ? *)").is_none());
        assert!(parse_schedule("cron(0 0 ? * BADDAY *)").is_none());
    }

    mod tick_tests {
        use super::*;
        use crate::state::{
            EventBridgeState, EventRule, EventTarget as EbTarget, RuleKey, SharedEventBridgeState,
        };
        use fakecloud_aws::arn::Arn;
        use fakecloud_core::delivery::{
            EventBridgeDelivery, KinesisDelivery, SnsDelivery, SqsDelivery, StepFunctionsDelivery,
        };
        use parking_lot::RwLock;
        use std::sync::Mutex;

        #[derive(Default)]
        struct Recorder {
            sqs: Mutex<Vec<(String, String)>>,
            sns: Mutex<Vec<(String, String)>>,
            stepfunctions: Mutex<Vec<(String, String)>>,
        }

        impl SqsDelivery for Recorder {
            fn deliver_to_queue(&self, arn: &str, body: &str, _attrs: &HashMap<String, String>) {
                self.sqs
                    .lock()
                    .unwrap()
                    .push((arn.to_string(), body.to_string()));
            }

            fn deliver_to_queue_with_attrs(
                &self,
                arn: &str,
                body: &str,
                _attrs: &HashMap<String, fakecloud_core::delivery::SqsMessageAttribute>,
                _group: Option<&str>,
                _dedup: Option<&str>,
            ) {
                self.sqs
                    .lock()
                    .unwrap()
                    .push((arn.to_string(), body.to_string()));
            }
        }

        impl SnsDelivery for Recorder {
            fn publish_to_topic(&self, arn: &str, msg: &str, _subject: Option<&str>) {
                self.sns
                    .lock()
                    .unwrap()
                    .push((arn.to_string(), msg.to_string()));
            }
        }

        impl StepFunctionsDelivery for Recorder {
            fn start_execution(&self, arn: &str, input: &str) {
                self.stepfunctions
                    .lock()
                    .unwrap()
                    .push((arn.to_string(), input.to_string()));
            }
        }

        impl EventBridgeDelivery for Recorder {
            fn put_event(&self, _source: &str, _detail_type: &str, _detail: &str, _bus: &str) {}
        }

        impl KinesisDelivery for Recorder {
            fn put_record(&self, _stream_arn: &str, _data: &str, _partition_key: &str) {}
        }

        fn make_state() -> (SharedEventBridgeState, EventBridgeState) {
            let state = EventBridgeState::new("123456789012", "us-east-1");
            let shared = Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            ));
            (shared, state)
        }

        fn make_rule(name: &str, schedule: &str, target_arn: &str) -> EventRule {
            EventRule {
                name: name.to_string(),
                arn: Arn::new(
                    "events",
                    "us-east-1",
                    "123456789012",
                    &format!("rule/{name}"),
                )
                .to_string(),
                event_bus_name: "default".to_string(),
                event_pattern: None,
                schedule_expression: Some(schedule.to_string()),
                state: "ENABLED".to_string(),
                description: None,
                role_arn: None,
                managed_by: None,
                created_by: None,
                targets: vec![EbTarget {
                    id: "t1".to_string(),
                    arn: target_arn.to_string(),
                    input: None,
                    input_path: None,
                    input_transformer: None,
                    sqs_parameters: None,
                    ..Default::default()
                }],
                tags: BTreeMap::new(),
                last_fired: None,
            }
        }

        fn build_scheduler(state: SharedEventBridgeState, recorder: Arc<Recorder>) -> Scheduler {
            let bus = Arc::new(
                DeliveryBus::new()
                    .with_sqs(recorder.clone())
                    .with_sns(recorder.clone())
                    .with_stepfunctions(recorder.clone()),
            );
            Scheduler::new(state, bus)
        }

        #[test]
        fn tick_disabled_rule_is_skipped() {
            let (shared, _) = make_state();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let mut rule = make_rule("r", "rate(1 second)", "arn:aws:sqs:us-east-1:123:q");
                rule.state = "DISABLED".to_string();
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            assert!(recorder.sqs.lock().unwrap().is_empty());
        }

        #[test]
        fn tick_rule_without_targets_is_skipped() {
            let (shared, _) = make_state();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let mut rule = make_rule("r", "rate(1 second)", "arn:aws:sqs:us-east-1:123:q");
                rule.targets.clear();
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            assert!(recorder.sqs.lock().unwrap().is_empty());
        }

        #[test]
        fn tick_invalid_schedule_is_skipped() {
            let (shared, _) = make_state();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "bogus", "arn:aws:sqs:us-east-1:123:q");
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            assert!(recorder.sqs.lock().unwrap().is_empty());
        }

        #[test]
        fn tick_fires_rate_rule_to_sqs_target() {
            let (shared, _) = make_state();
            let q_arn = "arn:aws:sqs:us-east-1:123456789012:q1".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", &q_arn);
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let calls = recorder.sqs.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, q_arn);
            let payload: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
            assert_eq!(payload["detail-type"], "Scheduled Event");
            assert_eq!(payload["source"], "aws.events");
        }

        #[test]
        fn tick_delivers_constant_input_to_sqs() {
            // H3: the scheduler now reuses the shared dispatch, so a target
            // with a constant Input delivers that constant instead of the
            // full scheduled-event envelope.
            let (shared, _) = make_state();
            let q_arn = "arn:aws:sqs:us-east-1:123456789012:q-input".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let mut rule = make_rule("r", "rate(1 second)", &q_arn);
                rule.targets[0].input = Some("{\"constant\":42}".to_string());
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let calls = recorder.sqs.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, q_arn);
            assert_eq!(calls[0].1, "{\"constant\":42}");
        }

        #[test]
        fn tick_delivers_to_kinesis_target() {
            // H3: the scheduler previously had no Kinesis branch.
            let (shared, _) = make_state();
            let stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/s".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", &stream_arn);
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let bus = Arc::new(DeliveryBus::new().with_kinesis(recorder.clone()));
            let scheduler = Scheduler::new(shared.clone(), bus);
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            // Should not panic and should mark the rule fired.
            scheduler.tick(&mut last);
            let mas = shared.read();
            let rule = mas
                .default_ref()
                .rules
                .get(&("default".to_string(), "r".to_string()))
                .unwrap();
            assert!(rule.last_fired.is_some());
        }

        #[test]
        fn tick_fires_to_sns_target() {
            let (shared, _) = make_state();
            let topic_arn = "arn:aws:sns:us-east-1:123456789012:t1".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", &topic_arn);
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let calls = recorder.sns.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, topic_arn);
        }

        #[test]
        fn tick_fires_to_stepfunctions_target() {
            let (shared, _) = make_state();
            let sm_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:m1".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", &sm_arn);
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let calls = recorder.stepfunctions.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, sm_arn);
            let _mas = shared.read();
            let guard = _mas.default_ref();

            assert_eq!(guard.step_function_executions.len(), 1);
        }

        #[test]
        fn tick_lambda_target_records_invocation() {
            let (shared, _) = make_state();
            let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:F".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", &fn_arn);
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let _mas = shared.read();
            let guard = _mas.default_ref();

            assert_eq!(guard.lambda_invocations.len(), 1);
            assert_eq!(guard.lambda_invocations[0].function_arn, fn_arn);
        }

        #[test]
        fn tick_logs_target_records_delivery() {
            let (shared, _) = make_state();
            let lg_arn = "arn:aws:logs:us-east-1:123456789012:log-group:lg".to_string();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", &lg_arn);
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let _mas = shared.read();
            let guard = _mas.default_ref();

            assert_eq!(guard.log_deliveries.len(), 1);
            assert_eq!(guard.log_deliveries[0].log_group_arn, lg_arn);
        }

        #[test]
        fn tick_updates_last_fired() {
            let (shared, _) = make_state();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule("r", "rate(1 second)", "arn:aws:sqs:us-east-1:123:q");
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let scheduler = build_scheduler(shared.clone(), recorder.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick(&mut last);
            let _mas = shared.read();
            let guard = _mas.default_ref();

            let rule = guard
                .rules
                .get(&("default".to_string(), "r".to_string()))
                .unwrap();
            assert!(rule.last_fired.is_some());
        }

        /// Records every `save` and keeps the last payload so the test can
        /// assert the snapshot actually carries the advanced `last_fired`.
        #[derive(Default)]
        struct RecordingStore {
            saves: std::sync::atomic::AtomicUsize,
            last: Mutex<Option<Vec<u8>>>,
        }

        impl fakecloud_persistence::SnapshotStore for RecordingStore {
            fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
                Ok(None)
            }
            fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
                self.saves.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                *self.last.lock().unwrap() = Some(bytes.to_vec());
                Ok(())
            }
        }

        /// Regression (bug-hunt Tier 0 side-channel-persistence): firing a rule
        /// advances `last_fired` under the scheduler's own write lock, outside
        /// the service's snapshot path. Without writing through, on-disk
        /// `last_fired` stays `None` and a `rate(...)` rule double-fires on
        /// restart. A mutating tick must persist, and the snapshot must carry
        /// the advanced `last_fired`.
        #[tokio::test]
        async fn tick_persists_last_fired_through_snapshot_store() {
            use std::sync::atomic::Ordering::SeqCst;

            let (shared, _) = make_state();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let rule = make_rule(
                    "r",
                    "rate(1 second)",
                    "arn:aws:sqs:us-east-1:123456789012:q",
                );
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let recorder = Arc::new(Recorder::default());
            let store = Arc::new(RecordingStore::default());
            let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
            let scheduler = Scheduler::new(shared.clone(), bus).with_snapshot_store(store.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick_and_persist(&mut last).await;

            // last_fired advanced in memory...
            {
                let mas = shared.read();
                assert!(mas
                    .default_ref()
                    .rules
                    .get(&("default".to_string(), "r".to_string()))
                    .unwrap()
                    .last_fired
                    .is_some());
            }
            // ...and that advance was written through the snapshot store, with
            // the persisted rule carrying the new last_fired (so it survives a
            // restart instead of double-firing).
            assert!(
                store.saves.load(SeqCst) >= 1,
                "a mutating tick must persist the last_fired advance"
            );
            let bytes = store
                .last
                .lock()
                .unwrap()
                .clone()
                .expect("snapshot written");
            let snap: crate::state::EventBridgeSnapshot =
                serde_json::from_slice(&bytes).expect("snapshot deserializes");
            let accounts = snap.accounts.expect("v2 multi-account snapshot");
            let persisted_rule = accounts
                .get("123456789012")
                .expect("account persisted")
                .rules
                .get(&("default".to_string(), "r".to_string()))
                .expect("rule persisted");
            assert!(
                persisted_rule.last_fired.is_some(),
                "the on-disk rule must carry the advanced last_fired"
            );
        }

        /// An idle tick (nothing fires) must NOT write a snapshot every second.
        #[tokio::test]
        async fn idle_tick_does_not_persist() {
            let (shared, _) = make_state();
            {
                let mut s_accounts = shared.write();
                let s = s_accounts.default_mut();
                let mut rule = make_rule("r", "rate(1 second)", "arn:aws:sqs:us-east-1:123:q");
                rule.state = "DISABLED".to_string();
                s.rules
                    .insert(("default".to_string(), "r".to_string()), rule);
            }
            let store = Arc::new(RecordingStore::default());
            let bus = Arc::new(DeliveryBus::new());
            let scheduler = Scheduler::new(shared.clone(), bus).with_snapshot_store(store.clone());
            let mut last = HashMap::<RuleKey, (u32, u32)>::new();
            scheduler.tick_and_persist(&mut last).await;
            assert_eq!(
                store.saves.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "an idle tick must not snapshot"
            );
        }
    }
}
