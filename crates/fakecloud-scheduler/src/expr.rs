//! Schedule expression parsing and matching.
//!
//! EventBridge Scheduler supports three expression forms:
//! - `at(yyyy-mm-ddThh:mm:ss)` — one-shot execution at a specific instant
//! - `rate(N unit)` — recurring every N minutes|hours|days (SECONDS NOT
//!   allowed by AWS, but fakecloud accepts them for fast-iteration tests)
//! - `cron(min hour dom month dow year)` — six-field recurring expression
//!
//! The matching logic mirrors the simplified cron implementation in
//! `fakecloud-eventbridge::scheduler`: each field is either a wildcard
//! (`*` / `?`) or a single numeric value. Full cron syntax (ranges,
//! lists, step values) is not currently supported — the firing loop
//! simply skips schedules whose expressions don't match this grammar.

use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use std::time::Duration;

/// Parsed schedule expression.
#[derive(Debug, Clone)]
pub enum Expr {
    /// One-shot `at(...)` expression, resolved to a wall-clock instant.
    At(DateTime<Utc>),
    /// Recurring `rate(...)` expression.
    Rate(Duration),
    /// Recurring `cron(...)` expression.
    Cron(CronExpr),
}

#[derive(Debug, Clone)]
pub struct CronExpr {
    pub minute: CronField,
    pub hour: CronField,
    pub day_of_month: CronField,
    pub month: CronField,
    pub day_of_week: CronField,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronField {
    /// `*` or `?` — matches any value.
    Any,
    /// Comma-separated list of terms; matches when ANY term matches.
    Terms(Vec<CronTerm>),
}

/// One comma-separated element of a cron field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronTerm {
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

// Day-of-week names normalized to 0=Sunday..6=Saturday.
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
            // AWS Scheduler day-of-week: 1=SUN..7=SAT -> 0=SUN..6=SAT.
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

/// Parse a schedule expression. Returns `None` on any shape we don't
/// recognize — the firing loop treats unparseable schedules as
/// permanently disabled (they never fire, they never error).
pub fn parse(expr: &str) -> Option<Expr> {
    let expr = expr.trim();
    if let Some(inner) = expr.strip_prefix("at(").and_then(|s| s.strip_suffix(')')) {
        return parse_at(inner.trim());
    }
    if let Some(inner) = expr.strip_prefix("rate(").and_then(|s| s.strip_suffix(')')) {
        return parse_rate(inner.trim());
    }
    if let Some(inner) = expr.strip_prefix("cron(").and_then(|s| s.strip_suffix(')')) {
        return parse_cron(inner.trim());
    }
    None
}

fn parse_at(inner: &str) -> Option<Expr> {
    // AWS docs: at(yyyy-mm-ddThh:mm:ss) — no timezone, no fractional seconds
    let dt = NaiveDateTime::parse_from_str(inner, "%Y-%m-%dT%H:%M:%S").ok()?;
    Some(Expr::At(Utc.from_utc_datetime(&dt)))
}

fn parse_rate(inner: &str) -> Option<Expr> {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }
    let value: u64 = parts[0].parse().ok()?;
    let secs = match parts[1] {
        "second" | "seconds" => value,
        "minute" | "minutes" => value * 60,
        "hour" | "hours" => value * 3600,
        "day" | "days" => value * 86400,
        _ => return None,
    };
    Some(Expr::Rate(Duration::from_secs(secs)))
}

fn parse_cron(inner: &str) -> Option<Expr> {
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 6 {
        return None;
    }
    // Support single values, ranges (9-17), lists (0,30), steps (*/5,
    // 9-17/2), and JAN/MON-style names; reject malformed/out-of-range
    // fields. Previously ranges/lists/steps were rejected entirely, so
    // valid AWS schedules were silently disabled (bug-audit 2026-05-28,
    // 1.3). `year` (parts[5]) is parsed/validated but not enforced at
    // fire time — matches the eventbridge ticker contract.
    let cron = CronExpr {
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
        day_of_week: parse_cron_field(
            parts[4],
            &FieldSpec {
                min: 0,
                max: 6,
                names: DOW_NAMES,
            },
            true,
        )?,
    };
    // Validate the year field (parsed for correctness, not stored — the
    // ticker contract doesn't enforce year at fire time). A malformed year
    // still rejects the expression so CreateSchedule surfaces an error.
    parse_cron_field(
        parts[5],
        &FieldSpec {
            min: 1970,
            max: 2199,
            names: &[],
        },
        false,
    )?;
    Some(Expr::Cron(cron))
}

/// Parse one cron field, supporting `*`/`?`, single values, ranges,
/// steps, comma lists, and name aliases. `None` rejects the whole
/// expression for any malformed/out-of-range field.
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

/// Decide whether `expr` is due to fire, given its `last_fired` time
/// (if any) and the current wall clock `now`.
///
/// Contract per expression kind:
/// - `At(t)`: fires once when `now >= t` AND `last_fired` is `None`.
///   The ticker consumes this by deleting/disabling the schedule after
///   the first fire so subsequent ticks don't re-fire it.
/// - `Rate(d)`: fires if never fired (bootstraps on first tick), or if
///   `now - last_fired >= d`.
/// - `Cron(c)`: fires when every field matches the current minute AND
///   we haven't already fired within this same minute (ticker
///   dedupe lives outside this function — see `CronFireTracker`).
pub fn is_due(expr: &Expr, last_fired: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    match expr {
        Expr::At(t) => last_fired.is_none() && now >= *t,
        Expr::Rate(d) => match last_fired {
            Some(last) => {
                now.signed_duration_since(last)
                    .to_std()
                    .unwrap_or(Duration::ZERO)
                    >= *d
            }
            None => true,
        },
        Expr::Cron(c) => matches_cron(c, now),
    }
}

/// Check whether each cron field matches the fields of `now`. The
/// per-minute dedupe has to live in the ticker because we need to
/// track fires across multiple calls without mutating the cron state.
pub fn matches_cron(c: &CronExpr, now: DateTime<Utc>) -> bool {
    matches_cron_fields(
        c,
        now.minute(),
        now.hour(),
        now.day(),
        now.month(),
        now.weekday().num_days_from_sunday(),
    )
}

/// `matches_cron` evaluated against the local time in IANA timezone `tz`.
/// AWS Scheduler interprets cron in `ScheduleExpressionTimezone` rather
/// than UTC; unknown names fall back to UTC so we never silently lose a
/// schedule (the service layer rejects bad names at create time).
pub fn matches_cron_in_tz(c: &CronExpr, now: DateTime<Utc>, tz: &str) -> bool {
    match tz.parse::<chrono_tz::Tz>() {
        Ok(tz) => {
            let local = now.with_timezone(&tz);
            matches_cron_fields(
                c,
                local.minute(),
                local.hour(),
                local.day(),
                local.month(),
                local.weekday().num_days_from_sunday(),
            )
        }
        Err(_) => matches_cron(c, now),
    }
}

fn matches_cron_fields(
    c: &CronExpr,
    minute: u32,
    hour: u32,
    day: u32,
    month: u32,
    day_of_week: u32,
) -> bool {
    let match_field = |f: &CronField, actual: u32| -> bool {
        match f {
            CronField::Any => true,
            CronField::Terms(terms) => terms.iter().any(|t| term_matches(t, actual)),
        }
    };
    match_field(&c.minute, minute)
        && match_field(&c.hour, hour)
        && match_field(&c.day_of_month, day)
        && match_field(&c.month, month)
        && match_field(&c.day_of_week, day_of_week)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rate_forms() {
        assert!(matches!(parse("rate(1 minute)"), Some(Expr::Rate(_))));
        assert!(matches!(parse("rate(5 minutes)"), Some(Expr::Rate(_))));
        assert!(matches!(parse("rate(1 hour)"), Some(Expr::Rate(_))));
        assert!(matches!(parse("rate(2 days)"), Some(Expr::Rate(_))));
        assert!(matches!(parse("rate(1 second)"), Some(Expr::Rate(_))));
    }

    #[test]
    fn parse_rate_rejects_bad_unit_and_shape() {
        assert!(parse("rate(1 fortnight)").is_none());
        assert!(parse("rate(abc minutes)").is_none());
        assert!(parse("rate(5)").is_none());
    }

    #[test]
    fn parse_at_utc() {
        let e = parse("at(2030-01-01T12:00:00)").unwrap();
        match e {
            Expr::At(t) => assert_eq!(t.timestamp(), 1893499200),
            _ => panic!("expected At"),
        }
    }

    #[test]
    fn parse_at_rejects_garbage() {
        assert!(parse("at(2030-01-01)").is_none());
        assert!(parse("at(nope)").is_none());
        assert!(parse("at()").is_none());
    }

    #[test]
    fn parse_cron_shape() {
        assert!(matches!(parse("cron(* * * * ? *)"), Some(Expr::Cron(_))));
        assert!(matches!(parse("cron(0 12 * * ? *)"), Some(Expr::Cron(_))));
        assert!(parse("cron(1 2 3)").is_none());
    }

    #[test]
    fn parse_cron_supports_ranges_lists_steps() {
        // bug-audit 2026-05-28, 1.3: ranges/lists/steps are now real cron
        // syntax that parses and fires, instead of being rejected (so the
        // schedule silently never fired).
        assert!(matches!(parse("cron(*/5 * * * ? *)"), Some(Expr::Cron(_))));
        assert!(matches!(
            parse("cron(1,3,5 * * * ? *)"),
            Some(Expr::Cron(_))
        ));
        assert!(matches!(parse("cron(1-3 * * * ? *)"), Some(Expr::Cron(_))));
        assert!(matches!(
            parse("cron(0 9-17 ? * MON-FRI *)"),
            Some(Expr::Cron(_))
        ));
        // Malformed / out-of-range still rejected.
        assert!(parse("cron(xyz 12 * * ? *)").is_none());
        assert!(parse("cron(99 * * * ? *)").is_none());
        assert!(parse("cron(0 0 1 13 ? *)").is_none());
    }

    #[test]
    fn cron_ranges_and_steps_fire_correctly() {
        // 2026-06-01 = Monday.
        let Some(Expr::Cron(c)) = parse("cron(*/15 9-17 ? * MON-FRI *)") else {
            panic!("expected cron");
        };
        let at = |min, hour, dom, dow_ok: bool| {
            // pick a Monday (1st) or Saturday (6th) of June 2026
            let day = if dow_ok { 1 } else { 6 };
            Utc.with_ymd_and_hms(2026, 6, day, hour, min, 0).unwrap()
        };
        assert!(matches_cron(&c, at(0, 9, 1, true)));
        assert!(matches_cron(&c, at(15, 17, 1, true)));
        assert!(!matches_cron(&c, at(10, 9, 1, true))); // not /15
        assert!(!matches_cron(&c, at(0, 8, 1, true))); // before hour range
        assert!(!matches_cron(&c, at(0, 9, 6, false))); // Saturday
    }

    #[test]
    fn parse_unknown_returns_none() {
        assert!(parse("every day at noon").is_none());
        assert!(parse("").is_none());
        assert!(parse("at").is_none());
    }

    #[test]
    fn matches_cron_in_tz_uses_local_hour() {
        // 12:00 UTC == 04:00 America/Los_Angeles in winter.
        // A schedule of "cron(0 4 * * ? *)" in LA tz must match,
        // but the same schedule against the UTC-only matcher must miss.
        let now = Utc.with_ymd_and_hms(2026, 1, 15, 12, 0, 0).unwrap();
        let cron = parse("cron(0 4 * * ? *)").unwrap();
        let cron = match cron {
            Expr::Cron(c) => c,
            _ => panic!("expected Cron"),
        };
        assert!(matches_cron_in_tz(&cron, now, "America/Los_Angeles"));
        assert!(!matches_cron(&cron, now));
    }

    #[test]
    fn matches_cron_in_tz_falls_back_to_utc_for_unknown_zone() {
        // Hour 12 UTC matches "cron(0 12 * * ? *)" if tz is unparseable.
        let now = Utc.with_ymd_and_hms(2026, 1, 15, 12, 0, 0).unwrap();
        let cron = match parse("cron(0 12 * * ? *)").unwrap() {
            Expr::Cron(c) => c,
            _ => panic!("expected Cron"),
        };
        assert!(matches_cron_in_tz(&cron, now, "Not/A_Real_Zone"));
    }

    #[test]
    fn is_due_at_fires_once_after_target() {
        let t = Utc.with_ymd_and_hms(2030, 1, 1, 12, 0, 0).unwrap();
        let expr = Expr::At(t);
        assert!(!is_due(&expr, None, t - chrono::Duration::seconds(1)));
        assert!(is_due(&expr, None, t));
        assert!(is_due(&expr, None, t + chrono::Duration::seconds(10)));
        assert!(!is_due(&expr, Some(t), t + chrono::Duration::seconds(10)));
    }

    #[test]
    fn is_due_rate_fires_on_bootstrap_and_after_interval() {
        let expr = Expr::Rate(Duration::from_secs(60));
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert!(is_due(&expr, None, now));
        assert!(!is_due(
            &expr,
            Some(now),
            now + chrono::Duration::seconds(30)
        ));
        assert!(is_due(
            &expr,
            Some(now),
            now + chrono::Duration::seconds(60)
        ));
        assert!(is_due(
            &expr,
            Some(now),
            now + chrono::Duration::seconds(61)
        ));
    }

    #[test]
    fn is_due_cron_wildcards_always_match() {
        let c = CronExpr {
            minute: CronField::Any,
            hour: CronField::Any,
            day_of_month: CronField::Any,
            month: CronField::Any,
            day_of_week: CronField::Any,
        };
        let now = Utc.with_ymd_and_hms(2026, 5, 15, 10, 30, 0).unwrap();
        assert!(is_due(&Expr::Cron(c), None, now));
    }

    #[test]
    fn is_due_cron_specific_minute_mismatch() {
        let c = CronExpr {
            minute: CronField::Terms(vec![CronTerm::Single(45)]),
            hour: CronField::Any,
            day_of_month: CronField::Any,
            month: CronField::Any,
            day_of_week: CronField::Any,
        };
        let now = Utc.with_ymd_and_hms(2026, 5, 15, 10, 30, 0).unwrap();
        assert!(!is_due(&Expr::Cron(c), None, now));
    }
}
