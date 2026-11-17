//! Alarm mute rule schedules: when a rule is muting its alarms.
//!
//! A rule's `Schedule` is an expression, a duration and a time zone:
//!
//! - `cron(Minutes Hours Day-of-month Month Day-of-week)` starts a mute window
//!   at every matching local time (a sixth `Year` field is accepted too).
//!   Fields take `*`, `,`, `-` and `/`; day-of-month also `?` and `L` (last day
//!   of the month); day-of-week `?`, `L` (`5L`: the last Friday) and `#`
//!   (`SUN#4`: the fourth Sunday). Months take `JAN`-`DEC`, days of the week
//!   `SUN`-`SAT` or `0`-`7` (0 and 7 are Sunday). As in cron, when both
//!   day-of-month and day-of-week are restricted a day matching either
//!   matches.
//! - `at(yyyy-MM-ddThh:mm)` starts a single mute window.
//! - `Duration` is an ISO 8601 duration from `PT1M` to `P15D`: each window
//!   lasts that long from its start.
//! - `Timezone` (an IANA name, UTC by default) is where the expression's
//!   times are read. A local time skipped by a daylight-saving change starts
//!   no window, and a repeated one starts a single window, at its first
//!   occurrence.
//!
//! `StartDate` and `ExpireDate` bound the rule: it mutes nothing before the
//! start date or from the expire date on.
//!
//! A rule is `ACTIVE` while now is inside one of its windows (and inside
//! those bounds), `EXPIRED` once it can never be active again (past its
//! expire date, or a one-time rule past its window), and `SCHEDULED`
//! otherwise.

use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc,
};
use chrono_tz::Tz;

use crate::state::AlarmMuteRule;

/// `AlarmMuteRuleStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MuteRuleStatus {
    Scheduled,
    Active,
    Expired,
}

impl MuteRuleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MuteRuleStatus::Scheduled => "SCHEDULED",
            MuteRuleStatus::Active => "ACTIVE",
            MuteRuleStatus::Expired => "EXPIRED",
        }
    }
}

/// The parsed schedule of a mute rule.
#[derive(Debug, Clone)]
pub(crate) struct MuteSchedule {
    pub expression: MuteExpression,
    pub duration: Duration,
    pub timezone: Tz,
}

#[derive(Debug, Clone)]
pub(crate) enum MuteExpression {
    /// A one-time window starting at this local time.
    At(NaiveDateTime),
    /// A window at every matching local time.
    Cron(Box<Cron>),
}

/// `ONE_TIME` for an `at` expression, `RECURRING` for a `cron` one.
pub(crate) fn mute_type(expression: Option<&str>) -> &'static str {
    match expression {
        Some(e) if e.trim_start().starts_with("at(") => "ONE_TIME",
        _ => "RECURRING",
    }
}

/// Parse a rule's schedule. `None` when the expression, duration or time
/// zone isn't valid; such a rule never mutes.
pub(crate) fn parse_schedule(
    expression: &str,
    duration: &str,
    timezone: Option<&str>,
) -> Option<MuteSchedule> {
    let expression = parse_expression(expression)?;
    let duration = parse_duration(duration)?;
    let timezone = match timezone {
        None => Tz::UTC,
        Some(tz) => tz.trim().parse::<Tz>().ok()?,
    };
    Some(MuteSchedule {
        expression,
        duration,
        timezone,
    })
}

/// Why a schedule given to PutAlarmMuteRule is invalid, if it is.
pub(crate) fn schedule_error(
    expression: &str,
    duration: Option<&str>,
    timezone: Option<&str>,
) -> Option<String> {
    if parse_expression(expression).is_none() {
        return Some(format!(
            "Invalid schedule expression {expression}: use cron(Minutes Hours Day-of-month \
             Month Day-of-week) or at(yyyy-MM-ddThh:mm)"
        ));
    }
    let Some(duration) = duration else {
        return Some("The schedule duration is required".to_string());
    };
    if parse_duration(duration).is_none() {
        return Some(format!(
            "Invalid schedule duration {duration}: use an ISO 8601 duration from PT1M to P15D"
        ));
    }
    if let Some(tz) = timezone {
        if tz.trim().parse::<Tz>().is_err() {
            return Some(format!("Invalid time zone {tz}"));
        }
    }
    None
}

fn rule_schedule(rule: &AlarmMuteRule) -> Option<MuteSchedule> {
    parse_schedule(
        rule.schedule_expression.as_deref()?,
        rule.schedule_duration.as_deref()?,
        rule.schedule_timezone.as_deref(),
    )
}

/// A rule's status at `now`.
pub(crate) fn rule_status(rule: &AlarmMuteRule, now: DateTime<Utc>) -> MuteRuleStatus {
    if rule.expire_date.is_some_and(|e| now >= e) {
        return MuteRuleStatus::Expired;
    }
    let Some(schedule) = rule_schedule(rule) else {
        return MuteRuleStatus::Scheduled;
    };
    schedule_status(&schedule, rule.start_date, rule.expire_date, now)
}

/// The window of a one-time schedule clipped to `start` / `expire`, or
/// `None` when it never opens (a local time the time zone skips, or bounds
/// leaving nothing of it).
fn one_time_window(
    schedule: &MuteSchedule,
    local: NaiveDateTime,
    start: Option<DateTime<Utc>>,
    expire: Option<DateTime<Utc>>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let begin = resolve_local(&schedule.timezone, local)?;
    let mut end = begin.checked_add_signed(schedule.duration)?;
    if let Some(e) = expire {
        end = end.min(e);
    }
    let begin = start.map_or(begin, |s| begin.max(s));
    (begin < end).then_some((begin, end))
}

/// Whether `schedule` bounded by `start` / `expire` mutes at `now`.
pub(crate) fn schedule_active(
    schedule: &MuteSchedule,
    start: Option<DateTime<Utc>>,
    expire: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    if expire.is_some_and(|e| now >= e) || start.is_some_and(|s| now < s) {
        return false;
    }
    match &schedule.expression {
        MuteExpression::At(local) => one_time_window(schedule, *local, start, expire)
            .is_some_and(|(begin, end)| now >= begin && now < end),
        MuteExpression::Cron(cron) => {
            let Some(lookback) = schedule.duration.checked_add(&Duration::days(2)) else {
                return false;
            };
            cron.latest_at_or_before(&schedule.timezone, now, lookback)
                .and_then(|occ| occ.checked_add_signed(schedule.duration))
                .is_some_and(|end| now < end)
        }
    }
}

/// The status of `schedule` bounded by `start` / `expire` at `now`.
pub(crate) fn schedule_status(
    schedule: &MuteSchedule,
    start: Option<DateTime<Utc>>,
    expire: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> MuteRuleStatus {
    if expire.is_some_and(|e| now >= e) {
        return MuteRuleStatus::Expired;
    }
    if schedule_active(schedule, start, expire, now) {
        return MuteRuleStatus::Active;
    }
    match &schedule.expression {
        MuteExpression::At(local) => match one_time_window(schedule, *local, start, expire) {
            Some((_, end)) if now < end => MuteRuleStatus::Scheduled,
            _ => MuteRuleStatus::Expired,
        },
        MuteExpression::Cron(cron) => {
            // Not muting now: is there a window still to come? The first one
            // ending after `from` (a window already running at the start
            // date counts from the start date), if it begins before the
            // expire date.
            let from = match start {
                Some(s) if s > now => s,
                _ => now,
            };
            let Some(search_from) = from.checked_sub_signed(schedule.duration) else {
                return MuteRuleStatus::Expired;
            };
            match cron.next_after(&schedule.timezone, search_from, expire) {
                Some(next) if expire.is_none_or(|e| next.max(from) < e) => {
                    MuteRuleStatus::Scheduled
                }
                _ => MuteRuleStatus::Expired,
            }
        }
    }
}

/// Whether `rule` mutes its alarms at `now`.
pub(crate) fn rule_active(rule: &AlarmMuteRule, now: DateTime<Utc>) -> bool {
    if rule.expire_date.is_some_and(|e| now >= e) {
        return false;
    }
    rule_schedule(rule).is_some_and(|s| schedule_active(&s, rule.start_date, rule.expire_date, now))
}

/// The UTC instant of a local time in `tz`: the first one when a clock
/// change repeats it, `None` when a clock change skips it.
fn resolve_local(tz: &Tz, local: NaiveDateTime) -> Option<DateTime<Utc>> {
    match tz.from_local_datetime(&local) {
        LocalResult::Single(t) => Some(t.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => Some(first.min(second).with_timezone(&Utc)),
        LocalResult::None => None,
    }
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

fn parse_expression(expr: &str) -> Option<MuteExpression> {
    let expr = expr.trim();
    if let Some(inner) = expr.strip_prefix("at(").and_then(|s| s.strip_suffix(')')) {
        let inner = inner.trim();
        // `yyyy`: four digits, no sign.
        if !inner.as_bytes().get(..4)?.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let local = NaiveDateTime::parse_from_str(inner, "%Y-%m-%dT%H:%M")
            .or_else(|_| NaiveDateTime::parse_from_str(inner, "%Y-%m-%dT%H:%M:%S"))
            .ok()?;
        if !(1970..=9999).contains(&local.year()) {
            return None;
        }
        return Some(MuteExpression::At(local));
    }
    if let Some(inner) = expr.strip_prefix("cron(").and_then(|s| s.strip_suffix(')')) {
        return Cron::parse(inner).map(|c| MuteExpression::Cron(Box::new(c)));
    }
    None
}

/// An ISO 8601 duration (`P[nW][nD][T[nH][nM][nS]]`) between one minute and
/// fifteen days. Years and months have no fixed length, so they're refused.
pub(crate) fn parse_duration(s: &str) -> Option<Duration> {
    let rest = s.trim().strip_prefix('P')?;
    let (date, time) = match rest.split_once('T') {
        Some((d, t)) => {
            if t.is_empty() {
                return None;
            }
            (d, Some(t))
        }
        None => (rest, None),
    };
    // Seconds, checked: anything past the 15-day maximum is refused before
    // it can overflow.
    const MAX_SECS: i64 = 15 * 86_400;
    let mut total: i64 = 0;
    let mut any = false;
    let mut parse_part = |part: &str, units: &[(char, i64)]| -> Option<()> {
        let mut num = String::new();
        let mut last_unit = None::<usize>;
        for c in part.chars() {
            if c.is_ascii_digit() {
                num.push(c);
                continue;
            }
            let idx = units.iter().position(|(u, _)| *u == c)?;
            // Units in order, each once.
            if last_unit.is_some_and(|l| idx <= l) || num.is_empty() {
                return None;
            }
            let n: i64 = num.parse().ok()?;
            total = total.checked_add(n.checked_mul(units[idx].1)?)?;
            if total > MAX_SECS {
                return None;
            }
            num.clear();
            last_unit = Some(idx);
            any = true;
        }
        num.is_empty().then_some(())
    };
    parse_part(date, &[('W', 7 * 86_400), ('D', 86_400)])?;
    if let Some(t) = time {
        parse_part(t, &[('H', 3_600), ('M', 60), ('S', 1)])?;
    }
    if !any || !(60..=MAX_SECS).contains(&total) {
        return None;
    }
    Duration::try_seconds(total)
}

/// One `lo-hi/step` term of a cron field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Term {
    lo: u32,
    hi: u32,
    step: u32,
}

impl Term {
    fn matches(&self, v: u32) -> bool {
        v >= self.lo && v <= self.hi && (v - self.lo).is_multiple_of(self.step)
    }
}

/// A numeric cron field.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    /// `*` or `?`: not restricted.
    any: bool,
    terms: Vec<Term>,
}

impl Field {
    fn matches(&self, v: u32) -> bool {
        self.any || self.terms.iter().any(|t| t.matches(v))
    }
}

/// The day-of-month field.
#[derive(Debug, Clone)]
struct DayOfMonth {
    field: Field,
    /// `L`: the last day of the month.
    last: bool,
}

/// The day-of-week field (0 = Sunday .. 6 = Saturday; 7 is Sunday too).
#[derive(Debug, Clone)]
struct DayOfWeek {
    field: Field,
    /// `nL`: the last such weekday of the month.
    last: Vec<u32>,
    /// `n#k`: the k-th such weekday of the month.
    nth: Vec<(u32, u32)>,
}

/// A parsed cron expression.
#[derive(Debug, Clone)]
pub(crate) struct Cron {
    minutes: Field,
    hours: Field,
    dom: DayOfMonth,
    months: Field,
    dow: DayOfWeek,
    years: Field,
}

const MONTHS: &[&str] = &[
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DAYS: &[&str] = &["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

fn value(tok: &str, lo: u32, hi: u32, names: &[&str], name_base: u32) -> Option<u32> {
    let v = match tok.parse::<u32>() {
        Ok(n) => n,
        Err(_) => {
            let upper = tok.to_ascii_uppercase();
            names.iter().position(|n| *n == upper)? as u32 + name_base
        }
    };
    (lo..=hi).contains(&v).then_some(v)
}

/// Parse a numeric field (`*`, `?` when `question` is allowed, lists, ranges
/// and steps), with `names` (`JAN`.. / `SUN`..) mapped from `name_base`.
fn parse_field(
    s: &str,
    lo: u32,
    hi: u32,
    names: &[&str],
    name_base: u32,
    question: bool,
) -> Option<Field> {
    if s == "*" || (question && s == "?") {
        return Some(Field {
            any: true,
            terms: Vec::new(),
        });
    }
    let mut terms = Vec::new();
    for part in s.split(',') {
        terms.push(parse_term(part, lo, hi, names, name_base)?);
    }
    Some(Field { any: false, terms })
}

fn parse_term(part: &str, lo: u32, hi: u32, names: &[&str], name_base: u32) -> Option<Term> {
    let (range, step) = match part.split_once('/') {
        Some((r, st)) => {
            let step: u32 = st.parse().ok()?;
            if step == 0 {
                return None;
            }
            (r, Some(step))
        }
        None => (part, None),
    };
    let (a, b) = if range == "*" {
        (lo, hi)
    } else if let Some((x, y)) = range.split_once('-') {
        let a = value(x, lo, hi, names, name_base)?;
        let b = value(y, lo, hi, names, name_base)?;
        if a > b {
            return None;
        }
        (a, b)
    } else {
        let a = value(range, lo, hi, names, name_base)?;
        // `a/n` runs from a to the end of the field.
        (a, if step.is_some() { hi } else { a })
    };
    Some(Term {
        lo: a,
        hi: b,
        step: step.unwrap_or(1),
    })
}

impl Cron {
    pub(crate) fn parse(inner: &str) -> Option<Cron> {
        let fields: Vec<&str> = inner.split_whitespace().collect();
        if fields.len() != 5 && fields.len() != 6 {
            return None;
        }
        let minutes = parse_field(fields[0], 0, 59, &[], 0, false)?;
        let hours = parse_field(fields[1], 0, 23, &[], 0, false)?;
        let dom = parse_dom(fields[2])?;
        let months = parse_field(fields[3], 1, 12, MONTHS, 1, false)?;
        let dow = parse_dow(fields[4])?;
        let years = match fields.get(5) {
            Some(y) => parse_field(y, 1970, 2199, &[], 0, true)?,
            None => Field {
                any: true,
                terms: Vec::new(),
            },
        };
        Some(Cron {
            minutes,
            hours,
            dom,
            months,
            dow,
            years,
        })
    }

    /// Whether a window starts on local date `d` (ignoring the time).
    fn day_matches(&self, d: NaiveDate) -> bool {
        if !self.months.matches(d.month()) || !self.years.matches(d.year() as u32) {
            return false;
        }
        let dom_restricted = !self.dom.field.any || self.dom.last;
        let dow_restricted =
            !self.dow.field.any || !self.dow.last.is_empty() || !self.dow.nth.is_empty();
        let dom_ok = self.dom_matches(d);
        let dow_ok = self.dow_matches(d);
        match (dom_restricted, dow_restricted) {
            (true, true) => dom_ok || dow_ok,
            (true, false) => dom_ok,
            (false, true) => dow_ok,
            (false, false) => true,
        }
    }

    fn dom_matches(&self, d: NaiveDate) -> bool {
        if self.dom.last && d.day() == last_day_of_month(d) {
            return true;
        }
        if self.dom.field.any {
            return !self.dom.last;
        }
        self.dom.field.matches(d.day())
    }

    fn dow_matches(&self, d: NaiveDate) -> bool {
        let wd = d.weekday().num_days_from_sunday();
        let restricted_extra = !self.dow.last.is_empty() || !self.dow.nth.is_empty();
        let plain = if self.dow.field.any {
            !restricted_extra
        } else {
            self.dow.field.matches(wd) || (wd == 0 && self.dow.field.matches(7))
        };
        if plain {
            return true;
        }
        if self.dow.last.contains(&wd) && d.day() + 7 > last_day_of_month(d) {
            return true;
        }
        let nth = (d.day() - 1) / 7 + 1;
        self.dow.nth.iter().any(|&(w, k)| w == wd && k == nth)
    }

    /// The matching local times of local date `d`, latest first.
    fn times_desc(&self) -> impl Iterator<Item = NaiveTime> + '_ {
        (0..24u32)
            .rev()
            .filter(|h| self.hours.matches(*h))
            .flat_map(move |h| {
                (0..60u32)
                    .rev()
                    .filter(|m| self.minutes.matches(*m))
                    .filter_map(move |m| NaiveTime::from_hms_opt(h, m, 0))
            })
    }

    fn times_asc(&self) -> impl Iterator<Item = NaiveTime> + '_ {
        (0..24u32)
            .filter(|h| self.hours.matches(*h))
            .flat_map(move |h| {
                (0..60u32)
                    .filter(|m| self.minutes.matches(*m))
                    .filter_map(move |m| NaiveTime::from_hms_opt(h, m, 0))
            })
    }

    /// The latest window start at or before `now`, looking back at most
    /// `lookback`.
    pub(crate) fn latest_at_or_before(
        &self,
        tz: &Tz,
        now: DateTime<Utc>,
        lookback: Duration,
    ) -> Option<DateTime<Utc>> {
        let floor = now.checked_sub_signed(lookback)?;
        // Local dates: from today back to the floor's date.
        let floor_day = floor.with_timezone(tz).date_naive();
        let mut day = now.with_timezone(tz).date_naive();
        while day >= floor_day {
            if self.day_matches(day) {
                for t in self.times_desc() {
                    let Some(at) = resolve_local(tz, day.and_time(t)) else {
                        continue;
                    };
                    if at <= now {
                        return (at >= floor).then_some(at);
                    }
                }
            }
            day = day.pred_opt()?;
        }
        None
    }

    /// The last year the year field allows.
    fn last_year(&self) -> i32 {
        if self.years.any {
            2199
        } else {
            self.years.terms.iter().map(|t| t.hi).max().unwrap_or(2199) as i32
        }
    }

    /// The first window start strictly after `after` (and before `until`,
    /// when given), as far ahead as the year field allows.
    pub(crate) fn next_after(
        &self,
        tz: &Tz,
        after: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Option<DateTime<Utc>> {
        let mut last = NaiveDate::from_ymd_opt(self.last_year(), 12, 31)?;
        if let Some(u) = until {
            last = last.min(u.with_timezone(tz).date_naive().succ_opt()?);
        }
        let mut day = after.with_timezone(tz).date_naive().pred_opt()?;
        while day <= last {
            // Whole months (and years) that can't match are skipped.
            if !self.years.matches(day.year() as u32) {
                day = NaiveDate::from_ymd_opt(day.year() + 1, 1, 1)?;
                continue;
            }
            if !self.months.matches(day.month()) {
                day = first_of_next_month(day)?;
                continue;
            }
            if self.day_matches(day) {
                for t in self.times_asc() {
                    let Some(at) = resolve_local(tz, day.and_time(t)) else {
                        continue;
                    };
                    if at > after {
                        return Some(at);
                    }
                }
            }
            day = day.succ_opt()?;
        }
        None
    }
}

fn parse_dom(s: &str) -> Option<DayOfMonth> {
    if s == "*" || s == "?" {
        return Some(DayOfMonth {
            field: Field {
                any: true,
                terms: Vec::new(),
            },
            last: false,
        });
    }
    let mut last = false;
    let mut terms = Vec::new();
    for part in s.split(',') {
        if part.eq_ignore_ascii_case("L") {
            last = true;
        } else {
            terms.push(parse_term(part, 1, 31, &[], 0)?);
        }
    }
    Some(DayOfMonth {
        // Only `L`: matches nothing else.
        field: Field { any: false, terms },
        last,
    })
}

fn parse_dow(s: &str) -> Option<DayOfWeek> {
    if s == "*" || s == "?" {
        return Some(DayOfWeek {
            field: Field {
                any: true,
                terms: Vec::new(),
            },
            last: Vec::new(),
            nth: Vec::new(),
        });
    }
    let day = |tok: &str| value(tok, 0, 7, DAYS, 0).map(|v| v % 7);
    let mut terms = Vec::new();
    let mut last = Vec::new();
    let mut nth = Vec::new();
    for part in s.split(',') {
        if part.eq_ignore_ascii_case("L") {
            // The last day of the week: Saturday.
            terms.push(Term {
                lo: 6,
                hi: 6,
                step: 1,
            });
        } else if let Some(d) = part.strip_suffix('L').or_else(|| part.strip_suffix('l')) {
            last.push(day(d)?);
        } else if let Some((d, k)) = part.split_once('#') {
            let k: u32 = k.parse().ok()?;
            if !(1..=5).contains(&k) {
                return None;
            }
            nth.push((day(d)?, k));
        } else {
            terms.push(parse_term(part, 0, 7, DAYS, 0)?);
        }
    }
    Some(DayOfWeek {
        field: Field { any: false, terms },
        last,
        nth,
    })
}

fn first_of_next_month(d: NaiveDate) -> Option<NaiveDate> {
    if d.month() == 12 {
        NaiveDate::from_ymd_opt(d.year() + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(d.year(), d.month() + 1, 1)
    }
}

fn last_day_of_month(d: NaiveDate) -> u32 {
    let (y, m) = if d.month() == 12 {
        (d.year() + 1, 1)
    } else {
        (d.year(), d.month() + 1)
    };
    NaiveDate::from_ymd_opt(y, m, 1)
        .and_then(|first| first.pred_opt())
        .map_or(31, |last| last.day())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn sched(expr: &str, dur: &str, tz: Option<&str>) -> MuteSchedule {
        parse_schedule(expr, dur, tz).unwrap_or_else(|| panic!("{expr} {dur} {tz:?}"))
    }

    fn status(s: &MuteSchedule, now: &str) -> MuteRuleStatus {
        schedule_status(s, None, None, utc(now))
    }

    use MuteRuleStatus::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("PT1M"), Some(Duration::minutes(1)));
        assert_eq!(parse_duration("PT4H"), Some(Duration::hours(4)));
        assert_eq!(parse_duration("P2DT12H"), Some(Duration::hours(60)));
        assert_eq!(parse_duration("P15D"), Some(Duration::days(15)));
        assert_eq!(parse_duration("P1W"), Some(Duration::days(7)));
        assert_eq!(parse_duration("PT1H30M15S"), Some(Duration::seconds(5415)));
        // Out of range, malformed, or non-fixed units.
        assert_eq!(parse_duration("PT59S"), None);
        assert_eq!(parse_duration("P16D"), None);
        assert_eq!(parse_duration("P1M"), None);
        assert_eq!(parse_duration("P1Y"), None);
        assert_eq!(parse_duration("P"), None);
        assert_eq!(parse_duration("PT"), None);
        assert_eq!(parse_duration("PT4"), None);
        assert_eq!(parse_duration("PTH"), None);
        assert_eq!(parse_duration("PT1M1H"), None);
        assert_eq!(parse_duration("4H"), None);
    }

    #[test]
    fn huge_durations_and_dates_are_refused_without_panicking() {
        assert_eq!(parse_duration("PT9300000000000000S"), None);
        assert_eq!(parse_duration("P99999999999999D"), None);
        assert_eq!(parse_duration("P9999999999999999999999W"), None);
        assert_eq!(parse_duration("PT2562047788015215H"), None);
        assert_eq!(parse_duration("P15DT1S"), None);
        assert!(parse_schedule("at(+262142-12-31T23:59)", "PT1H", None).is_none());
        assert!(parse_schedule("at(262142-12-31T23:59)", "PT1H", None).is_none());
        assert!(parse_schedule("at(-0001-01-01T00:00)", "PT1H", None).is_none());
        assert!(schedule_error("at(+262142-12-31T23:59)", Some("PT1H"), None).is_some());
        // The latest accepted time with the longest duration still works.
        let s = sched("at(9999-12-31T23:59)", "P15D", Some("Pacific/Kiritimati"));
        assert_eq!(status(&s, "2024-01-01T00:00:00Z"), Scheduled);
        let c = sched("cron(59 23 31 DEC ? 2199)", "P15D", None);
        assert_eq!(status(&c, "2024-01-01T00:00:00Z"), Scheduled);
        // A rule stored before validation existed, unparseable: never
        // active, never a panic.
        let rule = AlarmMuteRule {
            name: "r".into(),
            arn: "arn".into(),
            description: None,
            schedule_expression: Some("at(+262142-12-31T23:59)".into()),
            schedule_duration: Some("PT9300000000000000S".into()),
            schedule_timezone: None,
            mute_target_alarm_names: vec![],
            start_date: None,
            expire_date: None,
            last_updated_timestamp: utc("2024-01-01T00:00:00Z"),
            created_timestamp: None,
        };
        assert!(!rule_active(&rule, utc("2024-01-01T00:00:00Z")));
        assert_eq!(rule_status(&rule, utc("2024-01-01T00:00:00Z")), Scheduled);
    }

    #[test]
    fn far_future_years_are_scheduled() {
        // Beyond any fixed lookahead.
        let s = sched("cron(0 2 1 JAN ? 2040)", "PT1H", None);
        assert_eq!(status(&s, "2026-11-17T00:00:00Z"), Scheduled);
        assert_eq!(status(&s, "2040-01-01T02:30:00Z"), Active);
        assert_eq!(status(&s, "2040-01-01T03:00:00Z"), Expired);
        let late = sched("cron(0 2 1 JAN ? 2150-2199/7)", "PT1H", None);
        assert_eq!(status(&late, "2026-11-17T00:00:00Z"), Scheduled);
        // Unless the expire date comes first.
        assert_eq!(
            schedule_status(
                &s,
                None,
                Some(utc("2039-12-31T00:00:00Z")),
                utc("2026-11-17T00:00:00Z")
            ),
            Expired
        );
        // Only in leap years.
        let leap = sched("cron(0 0 29 FEB ?)", "PT1H", None);
        assert_eq!(status(&leap, "2026-11-17T00:00:00Z"), Scheduled);
        assert_eq!(status(&leap, "2028-02-29T00:30:00Z"), Active);
    }

    #[test]
    fn schedule_errors_name_the_problem() {
        assert!(schedule_error("cron(0 2 * * *)", Some("PT1H"), None).is_none());
        assert!(schedule_error("cron(0 2 * * *)", Some("PT1H"), Some("Asia/Tokyo")).is_none());
        assert!(schedule_error("cron(bogus)", Some("PT1H"), None)
            .unwrap()
            .contains("expression"));
        assert!(schedule_error("cron(0 2 * * *)", None, None)
            .unwrap()
            .contains("duration"));
        assert!(schedule_error("cron(0 2 * * *)", Some("forever"), None)
            .unwrap()
            .contains("duration"));
        assert!(
            schedule_error("cron(0 2 * * *)", Some("PT1H"), Some("Mars/Base"))
                .unwrap()
                .contains("time zone")
        );
    }

    #[test]
    fn invalid_schedules_dont_parse() {
        assert!(parse_schedule("rate(5 minutes)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 2 * *)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(60 2 * * *)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 24 * * *)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 2 32 * *)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 2 * 13 *)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 2 * * FUN)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 2 * * SUN#6)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(*/0 2 * * *)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(5-1 2 * * *)", "PT1H", None).is_none());
        assert!(parse_schedule("at(2024-13-01T00:00)", "PT1H", None).is_none());
        assert!(parse_schedule("at(2024-05-10 14:00)", "PT1H", None).is_none());
        assert!(parse_schedule("cron(0 2 * * *)", "PT1H", Some("Mars/Base")).is_none());
        assert!(parse_schedule("cron(0 2 * * *)", "1 hour", None).is_none());
        // The documented forms, plus the year field and seconds in `at`.
        assert!(parse_schedule("cron(0 2 * * ? *)", "PT1H", None).is_some());
        assert!(parse_schedule("cron(0 9 * * SUN#4)", "PT1H", None).is_some());
        assert!(parse_schedule("cron(0 9 L * ?)", "PT1H", None).is_some());
        assert!(parse_schedule("cron(0 9 ? * 5L)", "PT1H", None).is_some());
        assert!(parse_schedule("at(2024-05-10T14:00:00)", "PT1H", None).is_some());
    }

    #[test]
    fn mute_types() {
        assert_eq!(mute_type(Some("at(2024-05-10T14:00)")), "ONE_TIME");
        assert_eq!(mute_type(Some("cron(0 2 * * *)")), "RECURRING");
        assert_eq!(mute_type(None), "RECURRING");
    }

    #[test]
    fn one_time_window_boundaries() {
        let s = sched("at(2024-05-10T14:00)", "PT4H", None);
        assert_eq!(status(&s, "2024-05-10T13:59:59Z"), Scheduled);
        // The start is inside, the end outside.
        assert_eq!(status(&s, "2024-05-10T14:00:00Z"), Active);
        assert_eq!(status(&s, "2024-05-10T17:59:59Z"), Active);
        assert_eq!(status(&s, "2024-05-10T18:00:00Z"), Expired);
    }

    #[test]
    fn one_time_window_in_time_zone() {
        // 14:00 in New York (EDT, UTC-4) is 18:00 UTC.
        let s = sched("at(2024-05-10T14:00)", "PT1H", Some("America/New_York"));
        assert_eq!(status(&s, "2024-05-10T14:30:00Z"), Scheduled);
        assert_eq!(status(&s, "2024-05-10T18:30:00Z"), Active);
        assert_eq!(status(&s, "2024-05-10T19:00:00Z"), Expired);
        // Tokyo has no DST: 14:00 JST is 05:00 UTC.
        let t = sched("at(2024-12-23T14:00)", "P7D", Some("Asia/Tokyo"));
        assert_eq!(status(&t, "2024-12-23T04:59:00Z"), Scheduled);
        assert_eq!(status(&t, "2024-12-23T05:00:00Z"), Active);
        assert_eq!(status(&t, "2024-12-30T04:59:59Z"), Active);
        assert_eq!(status(&t, "2024-12-30T05:00:00Z"), Expired);
    }

    #[test]
    fn start_and_expire_dates_clip_windows() {
        let s = sched("at(2024-05-10T14:00)", "PT4H", None);
        let at = |now: &str, start: Option<&str>, expire: Option<&str>| {
            schedule_status(&s, start.map(utc), expire.map(utc), utc(now))
        };
        let start = Some("2024-05-10T16:00:00Z");
        assert_eq!(at("2024-05-10T15:00:00Z", start, None), Scheduled);
        assert_eq!(at("2024-05-10T16:00:00Z", start, None), Active);
        let expire = Some("2024-05-10T15:00:00Z");
        assert_eq!(at("2024-05-10T14:30:00Z", None, expire), Active);
        assert_eq!(at("2024-05-10T15:00:00Z", None, expire), Expired);
        // A start after the window: it never mutes.
        assert_eq!(
            at("2024-05-09T00:00:00Z", Some("2024-05-11T00:00:00Z"), None),
            Expired
        );
    }

    #[test]
    fn daily_cron_boundaries() {
        let s = sched("cron(0 23 * * *)", "PT2H", None);
        assert_eq!(status(&s, "2024-03-01T22:59:59Z"), Scheduled);
        assert_eq!(status(&s, "2024-03-01T23:00:00Z"), Active);
        // Across midnight into the next day.
        assert_eq!(status(&s, "2024-03-02T00:59:59Z"), Active);
        assert_eq!(status(&s, "2024-03-02T01:00:00Z"), Scheduled);
    }

    #[test]
    fn weekly_and_weekend_cron() {
        // Every Sunday 02:00-06:00. 2024-03-03 is a Sunday.
        let s = sched("cron(0 2 * * SUN)", "PT4H", None);
        assert_eq!(status(&s, "2024-03-03T03:00:00Z"), Active);
        assert_eq!(status(&s, "2024-03-04T03:00:00Z"), Scheduled);
        // 0 and 7 are both Sunday.
        let zero = sched("cron(0 2 * * 0)", "PT4H", None);
        let seven = sched("cron(0 2 * * 7)", "PT4H", None);
        assert_eq!(status(&zero, "2024-03-03T03:00:00Z"), Active);
        assert_eq!(status(&seven, "2024-03-03T03:00:00Z"), Active);
        // Friday 18:00 to Monday 06:00. 2024-03-01 is a Friday.
        let w = sched("cron(0 18 * * FRI)", "P2DT12H", None);
        assert_eq!(status(&w, "2024-03-01T17:59:00Z"), Scheduled);
        assert_eq!(status(&w, "2024-03-02T12:00:00Z"), Active);
        assert_eq!(status(&w, "2024-03-04T05:59:59Z"), Active);
        assert_eq!(status(&w, "2024-03-04T06:00:00Z"), Scheduled);
        // A range of weekdays.
        let wk = sched("cron(0 9 * * MON-FRI)", "PT1H", None);
        assert_eq!(status(&wk, "2024-03-01T09:30:00Z"), Active);
        assert_eq!(status(&wk, "2024-03-02T09:30:00Z"), Scheduled);
    }

    #[test]
    fn cron_lists_ranges_steps() {
        let s = sched("cron(*/15 8-10 * * *)", "PT5M", None);
        assert_eq!(status(&s, "2024-03-01T08:44:00Z"), Scheduled);
        assert_eq!(status(&s, "2024-03-01T08:45:00Z"), Active);
        assert_eq!(status(&s, "2024-03-01T10:49:59Z"), Active);
        assert_eq!(status(&s, "2024-03-01T11:00:00Z"), Scheduled);
        let l = sched("cron(0,30 12 * JAN,MAR *)", "PT10M", None);
        assert_eq!(status(&l, "2024-03-01T12:35:00Z"), Active);
        assert_eq!(status(&l, "2024-02-01T12:35:00Z"), Scheduled);
    }

    #[test]
    fn monthly_last_and_nth_weekday() {
        // First of the month, 01:00-07:00.
        let first = sched("cron(0 1 1 * *)", "PT6H", None);
        assert_eq!(status(&first, "2024-04-01T06:59:00Z"), Active);
        assert_eq!(status(&first, "2024-04-02T02:00:00Z"), Scheduled);
        // Last day of a leap February.
        let last = sched("cron(0 0 L * ?)", "PT1H", None);
        assert_eq!(status(&last, "2024-02-29T00:30:00Z"), Active);
        assert_eq!(status(&last, "2024-02-28T00:30:00Z"), Scheduled);
        // Fourth Sunday of March 2024 is the 24th.
        let fourth = sched("cron(0 9 * * SUN#4)", "PT1H", None);
        assert_eq!(status(&fourth, "2024-03-24T09:30:00Z"), Active);
        assert_eq!(status(&fourth, "2024-03-17T09:30:00Z"), Scheduled);
        assert_eq!(status(&fourth, "2024-03-31T09:30:00Z"), Scheduled);
        // Last Friday of March 2024 is the 29th.
        let last_fri = sched("cron(0 9 ? * 5L)", "PT1H", None);
        assert_eq!(status(&last_fri, "2024-03-29T09:30:00Z"), Active);
        assert_eq!(status(&last_fri, "2024-03-22T09:30:00Z"), Scheduled);
    }

    #[test]
    fn day_of_month_or_day_of_week() {
        // Both restricted: the 13th or any Friday.
        let s = sched("cron(0 0 13 * FRI)", "PT1H", None);
        // 2024-03-13 is a Wednesday, 2024-03-15 a Friday.
        assert_eq!(status(&s, "2024-03-13T00:30:00Z"), Active);
        assert_eq!(status(&s, "2024-03-15T00:30:00Z"), Active);
        assert_eq!(status(&s, "2024-03-14T00:30:00Z"), Scheduled);
    }

    #[test]
    fn cron_follows_time_zone_across_dst() {
        // 02:00 New York every day: 07:00 UTC in winter (EST), 06:00 UTC in
        // summer (EDT).
        let s = sched("cron(0 2 * * *)", "PT1H", Some("America/New_York"));
        assert_eq!(status(&s, "2024-03-01T07:30:00Z"), Active);
        assert_eq!(status(&s, "2024-03-01T06:30:00Z"), Scheduled);
        assert_eq!(status(&s, "2024-07-01T06:30:00Z"), Active);
        assert_eq!(status(&s, "2024-07-01T07:30:00Z"), Scheduled);
        // 2024-03-10: 02:00 local doesn't exist (clocks jump 02:00 -> 03:00),
        // so that day has no window.
        assert_eq!(status(&s, "2024-03-10T07:00:00Z"), Scheduled);
        assert_eq!(status(&s, "2024-03-10T06:30:00Z"), Scheduled);
        // The next day's window is at 06:00 UTC.
        assert_eq!(status(&s, "2024-03-11T06:30:00Z"), Active);
    }

    #[test]
    fn cron_repeated_local_time_starts_one_window() {
        // 2024-11-03: 01:30 New York happens at 05:30 UTC (EDT) and again at
        // 06:30 UTC (EST). The window starts at the first.
        let s = sched("cron(30 1 * * *)", "PT30M", Some("America/New_York"));
        assert_eq!(status(&s, "2024-11-03T05:29:00Z"), Scheduled);
        assert_eq!(status(&s, "2024-11-03T05:45:00Z"), Active);
        assert_eq!(status(&s, "2024-11-03T06:00:00Z"), Scheduled);
        assert_eq!(status(&s, "2024-11-03T06:45:00Z"), Scheduled);
        // One-time windows resolve the same way.
        let at = sched("at(2024-11-03T01:30)", "PT30M", Some("America/New_York"));
        assert_eq!(status(&at, "2024-11-03T05:45:00Z"), Active);
        assert_eq!(status(&at, "2024-11-03T06:45:00Z"), Expired);
        // A skipped local time never happens.
        let gap = sched("at(2024-03-10T02:30)", "PT1H", Some("America/New_York"));
        assert_eq!(status(&gap, "2024-03-01T00:00:00Z"), Expired);
    }

    #[test]
    fn london_time_zone() {
        // 23:00 London: 23:00 UTC in winter (GMT), 22:00 UTC in summer (BST).
        let s = sched("cron(0 23 * * *)", "PT2H", Some("Europe/London"));
        assert_eq!(status(&s, "2024-01-15T23:30:00Z"), Active);
        assert_eq!(status(&s, "2024-06-15T22:30:00Z"), Active);
        assert_eq!(status(&s, "2024-06-16T00:30:00Z"), Scheduled);
    }

    #[test]
    fn recurring_rule_expires() {
        let s = sched("cron(0 2 * * *)", "PT1H", None);
        let expire = Some(utc("2024-03-05T00:00:00Z"));
        assert_eq!(
            schedule_status(&s, None, expire, utc("2024-03-04T02:30:00Z")),
            Active
        );
        assert_eq!(
            schedule_status(&s, None, expire, utc("2024-03-04T03:30:00Z")),
            Expired
        );
        // Expire date in the middle of a window ends it.
        let mid = Some(utc("2024-03-04T02:30:00Z"));
        assert_eq!(
            schedule_status(&s, None, mid, utc("2024-03-04T02:29:00Z")),
            Active
        );
        assert_eq!(
            schedule_status(&s, None, mid, utc("2024-03-04T02:30:00Z")),
            Expired
        );
        // A year field in the past: never again.
        let past = sched("cron(0 2 * * ? 2020)", "PT1H", None);
        assert_eq!(status(&past, "2024-03-04T02:30:00Z"), Expired);
        // A day that never comes.
        let never = sched("cron(0 2 30 FEB *)", "PT1H", None);
        assert_eq!(status(&never, "2024-03-04T02:30:00Z"), Expired);
    }

    #[test]
    fn recurring_rule_before_start_date() {
        let s = sched("cron(0 2 * * *)", "PT1H", None);
        let start = Some(utc("2024-03-10T00:00:00Z"));
        // In a window, but before the start date.
        assert_eq!(
            schedule_status(&s, start, None, utc("2024-03-04T02:30:00Z")),
            Scheduled
        );
        assert_eq!(
            schedule_status(&s, start, None, utc("2024-03-10T02:30:00Z")),
            Active
        );
        // A start date inside a window mutes from the start date.
        let mid = Some(utc("2024-03-10T02:30:00Z"));
        assert_eq!(
            schedule_status(&s, mid, None, utc("2024-03-10T02:29:00Z")),
            Scheduled
        );
        assert_eq!(
            schedule_status(&s, mid, None, utc("2024-03-10T02:45:00Z")),
            Active
        );
        // Start and expire leave no window: expired.
        let expire = Some(utc("2024-03-10T01:00:00Z"));
        assert_eq!(
            schedule_status(
                &s,
                Some(utc("2024-03-09T03:00:00Z")),
                expire,
                utc("2024-03-01T00:00:00Z")
            ),
            Expired
        );
    }

    #[test]
    fn long_windows_overlap_and_look_back_far_enough() {
        // Every minute for 15 days: always active once started.
        let s = sched("cron(* * * * *)", "P15D", None);
        assert_eq!(status(&s, "2024-03-04T02:30:00Z"), Active);
        // Monthly 15-day window: still active 14 days in.
        let m = sched("cron(0 0 1 * *)", "P15D", None);
        assert_eq!(status(&m, "2024-03-15T23:59:00Z"), Active);
        assert_eq!(status(&m, "2024-03-16T00:00:00Z"), Scheduled);
    }
}
